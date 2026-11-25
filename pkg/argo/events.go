package argo

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// eventWatchTotalTimeout caps bounded WatchEvents snapshot collection in Argo Server mode.
	eventWatchTotalTimeout = 1500 * time.Millisecond
	// eventWatchIdleTimeout closes the stream once initial cached events finish draining.
	eventWatchIdleTimeout = 300 * time.Millisecond
	// maxCollectedEvents caps memory usage during snapshot reads.
	maxCollectedEvents = 200
)

// collectEventsViaWatch performs a bounded, short-lived WatchEvents snapshot against
// Argo Server (/api/v1/stream/events/{namespace}) so Argo Server mode stays 100% pure
// without requiring a Kubernetes ServiceAccount.
func collectEventsViaWatch(
	ctx context.Context,
	wfSvc workflow.WorkflowServiceClient,
	namespace string,
	workflowName string,
	podNames []string,
	onlyWarnings bool,
) ([]corev1.Event, error) {
	watchCtx, cancel := context.WithTimeout(ctx, eventWatchTotalTimeout)
	defer cancel()

	stream, err := wfSvc.WatchEvents(watchCtx, &workflow.WatchEventsRequest{
		Namespace:   namespace,
		ListOptions: &metav1.ListOptions{},
	})
	if err != nil {
		return nil, err
	}

	podSet := make(map[string]struct{}, len(podNames))
	for _, p := range podNames {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			podSet[trimmed] = struct{}{}
		}
	}

	type recvMsg struct {
		ev  *corev1.Event
		err error
	}
	ch := make(chan recvMsg, 1)
	go func() {
		defer close(ch)
		for {
			ev, recvErr := stream.Recv()
			select {
			case ch <- recvMsg{ev: ev, err: recvErr}:
			case <-watchCtx.Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()

	seen := make(map[string]corev1.Event)
	idleTimer := time.NewTimer(eventWatchIdleTimeout)
	defer idleTimer.Stop()

loop:
	for len(seen) < maxCollectedEvents {
		select {
		case msg, ok := <-ch:
			if !ok {
				break loop
			}
			if msg.err != nil {
				if errors.Is(msg.err, io.EOF) || errors.Is(msg.err, context.DeadlineExceeded) || errors.Is(msg.err, context.Canceled) {
					break loop
				}
				// If we already collected some events before stream closure, return them cleanly.
				if len(seen) > 0 {
					break loop
				}
				return nil, msg.err
			}
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(eventWatchIdleTimeout)

			if msg.ev != nil && matchesWorkflowEvent(msg.ev, workflowName, podSet, onlyWarnings) {
				seen[eventDedupKey(msg.ev)] = *msg.ev
			}
		case <-idleTimer.C:
			cancel()
			break loop
		case <-watchCtx.Done():
			break loop
		}
	}

	return sortEventsNewestFirst(seen), nil
}

func matchesWorkflowEvent(ev *corev1.Event, workflowName string, podSet map[string]struct{}, onlyWarnings bool) bool {
	if ev == nil {
		return false
	}
	if onlyWarnings && !strings.EqualFold(ev.Type, corev1.EventTypeWarning) {
		return false
	}
	if workflowName == "" && len(podSet) == 0 {
		return true
	}

	objName := ev.InvolvedObject.Name
	objKind := ev.InvolvedObject.Kind

	if objKind == "Workflow" && objName == workflowName {
		return true
	}
	if _, ok := podSet[objName]; ok {
		return true
	}
	if workflowName != "" && objKind == "Pod" && (objName == workflowName || strings.HasPrefix(objName, workflowName+"-")) {
		return true
	}
	return false
}

func eventDedupKey(ev *corev1.Event) string {
	return ev.Namespace + "/" + ev.InvolvedObject.Kind + "/" + ev.InvolvedObject.Name + "/" + ev.Reason + "/" + ev.Message
}

func eventTimestamp(ev corev1.Event) time.Time {
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	if !ev.FirstTimestamp.IsZero() {
		return ev.FirstTimestamp.Time
	}
	return ev.CreationTimestamp.Time
}

func sortEventsNewestFirst(m map[string]corev1.Event) []corev1.Event {
	out := make([]corev1.Event, 0, len(m))
	for _, ev := range m {
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := eventTimestamp(out[i]), eventTimestamp(out[j])
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return out[i].Name < out[j].Name
	})
	return out
}
