package governance

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// ProtocolVersionModern is the MCP 2026-07-28 revision supporting stateless MRTR.
	ProtocolVersionModern = "2026-07-28"

	defaultMRTRTTL      = 300 * time.Second
	mrtrKeyDomainPrefix = "mcp-for-argo-workflows:mrtr-request-state:v1:"
)

// ConfirmationState is the signed, stateless payload embedded in MRTR requestState
// so approval challenges issued by Pod A can be verified on Pod B.
type ConfirmationState struct {
	Tool      string `json:"tool"`
	Target    string `json:"target"`
	Digest    string `json:"digest"`
	Caller    string `json:"caller"`
	ExpiresAt int64  `json:"exp"`
}

// MRTRGate implements cross-replica HMAC-SHA256 write-confirmation challenges for
// 2026-07-28 clients and explicit confirmName fallback guards for 2025-xx clients.
type MRTRGate struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

// NewMRTRGate derives a deterministic 256-bit HMAC key from secret (or generates an
// ephemeral per-process key with a warning when unset).
func NewMRTRGate(secret string, ttl time.Duration) *MRTRGate {
	if ttl <= 0 {
		ttl = defaultMRTRTTL
	}
	return &MRTRGate{
		key: deriveMRTRKey(secret),
		ttl: ttl,
		now: time.Now,
	}
}

func deriveMRTRKey(secret string) []byte {
	trimmed := strings.TrimSpace(secret)
	if trimmed != "" {
		h := sha256.New()
		h.Write([]byte(mrtrKeyDomainPrefix))
		h.Write([]byte(trimmed))
		return h.Sum(nil)
	}
	slog.Warn("MCP_REQUEST_STATE_SECRET is unset; generating ephemeral per-process HMAC key (set a shared secret across replicas for multi-pod MRTR approvals)")
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// crypto/rand failure is fatal in practice; fallback still avoids panicking.
		h := sha256.Sum256([]byte(fmt.Sprintf("%s%d", mrtrKeyDomainPrefix, time.Now().UnixNano())))
		return h[:]
	}
	return key
}

// ComputeParamsDigest returns a short deterministic SHA-256 digest of confirmation parameters.
func ComputeParamsDigest(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:24]
}

// Mint signs state into a compact "<base64url-payload>.<base64url-hmac>" token.
func (g *MRTRGate) Mint(state ConfirmationState) (string, error) {
	if state.ExpiresAt == 0 {
		state.ExpiresAt = g.now().Add(g.ttl).Unix()
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("failed to marshal MRTR state: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(raw)
	sig := g.sign(payloadB64)
	return payloadB64 + "." + sig, nil
}

// Verify checks the HMAC signature and expiration of token and decodes ConfirmationState.
func (g *MRTRGate) Verify(token string) (*ConfirmationState, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("malformed requestState token")
	}
	expected := g.sign(parts[0])
	if !hmac.Equal([]byte(parts[1]), []byte(expected)) {
		return nil, fmt.Errorf("invalid requestState signature")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid requestState payload encoding: %w", err)
	}
	var state ConfirmationState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("invalid requestState payload: %w", err)
	}
	if g.now().Unix() > state.ExpiresAt {
		return nil, fmt.Errorf("confirmation requestState has expired; retry to obtain a fresh approval prompt")
	}
	return &state, nil
}

func (g *MRTRGate) sign(payload string) string {
	mac := hmac.New(sha256.New, g.key)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// ClientSupportsModernMRTR checks whether the active request runs on MCP 2026-07-28+
// AND advertises elicitation capability (either via session init params or per-request _meta).
func ClientSupportsModernMRTR(req *mcp.CallToolRequest) bool {
	if req == nil {
		return false
	}

	proto := ""
	if req.Session != nil {
		if initParams := req.Session.InitializeParams(); initParams != nil {
			proto = initParams.ProtocolVersion
		}
	}
	if proto == "" && req.Extra != nil && req.Extra.Header != nil {
		proto = strings.TrimSpace(req.Extra.Header.Get("Mcp-Protocol-Version"))
	}
	if proto < ProtocolVersionModern {
		return false
	}

	// Check session capabilities first.
	if req.Session != nil {
		if initParams := req.Session.InitializeParams(); initParams != nil &&
			initParams.Capabilities != nil &&
			initParams.Capabilities.Elicitation != nil {
			return true
		}
	}

	// Check 2026-07-28 per-request _meta client capabilities envelope.
	if req.Params != nil && req.Params.Meta != nil {
		if caps, ok := req.Params.Meta["io.modelcontextprotocol/clientCapabilities"].(map[string]any); ok {
			if el, exists := caps["elicitation"]; exists && el != nil && el != false {
				return true
			}
		}
	}

	return false
}

// ConfirmationRequest describes a destructive action requiring human approval.
type ConfirmationRequest struct {
	ToolName    string
	Target      string
	ConfirmName string
	Summary     string
	Digest      string
	Caller      string
	Req         *mcp.CallToolRequest
}

// Evaluate enforces dual-era destructive-operation confirmation:
//   - On 2026-07-28 clients with elicitation: returns a non-nil Challenge CallToolResult
//     (with InputRequests + HMAC-signed RequestState) until approved and verified.
//   - On 2025-xx clients: requires ConfirmName == Target and returns a descriptive error if absent/mismatched.
//
// Returns (challengeResult, err): when both are nil, the operation is approved to proceed.
func (g *MRTRGate) Evaluate(in ConfirmationRequest) (*mcp.CallToolResult, error) {
	if ClientSupportsModernMRTR(in.Req) {
		if in.Req.Params != nil && in.Req.Params.RequestState != "" {
			state, err := g.Verify(in.Req.Params.RequestState)
			if err != nil {
				return nil, fmt.Errorf("approval verification failed: %w", err)
			}
			if state.Tool != in.ToolName || state.Target != in.Target || state.Digest != in.Digest || state.Caller != in.Caller {
				return nil, fmt.Errorf("approval verification failed: requestState does not match tool parameters or caller")
			}
			if isAcceptedConfirmation(in.Req.Params.InputResponses) {
				return nil, nil
			}
			return nil, fmt.Errorf("operation cancelled: confirmation was not approved")
		}

		token, err := g.Mint(ConfirmationState{
			Tool:   in.ToolName,
			Target: in.Target,
			Digest: in.Digest,
			Caller: in.Caller,
		})
		if err != nil {
			return nil, err
		}

		schema := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"confirm": map[string]any{
					"type":        "boolean",
					"description": "Set to true to approve this destructive operation",
				},
			},
			"required": []string{"confirm"},
		}

		return &mcp.CallToolResult{
			RequestState: token,
			InputRequests: mcp.InputRequestMap{
				"confirm": &mcp.ElicitParams{
					Message:         in.Summary,
					RequestedSchema: schema,
				},
			},
		}, nil
	}

	// Legacy 2025-xx stateless fallback: require explicit confirmName matching the target identifier.
	if strings.TrimSpace(in.ConfirmName) != strings.TrimSpace(in.Target) {
		return nil, fmt.Errorf(
			"destructive action blocked: %s. Re-invoke with confirmName=%q after confirming with the user",
			in.Summary, in.Target,
		)
	}
	return nil, nil
}

func isAcceptedConfirmation(responses mcp.InputResponseMap) bool {
	if len(responses) == 0 {
		return false
	}
	raw, ok := responses["confirm"]
	if !ok || raw == nil {
		return false
	}
	elicitResp, ok := raw.(*mcp.ElicitResult)
	if !ok || elicitResp == nil {
		return false
	}
	if !strings.EqualFold(elicitResp.Action, "accept") {
		return false
	}
	v, ok := elicitResp.Content["confirm"].(bool)
	return ok && v
}
