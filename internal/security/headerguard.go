package security

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

const (
	// HeaderMismatchErrorCode mirrors argocd-mcp / SEP-2243 protocol desync rejection code.
	HeaderMismatchErrorCode = -32020
	// MaxGuardBodyBytes bounds request body reads in HeaderGuard.
	MaxGuardBodyBytes = 2 << 20 // 2 MiB
	// LegacyModeStateless accepts both 2026-07-28 and 2025-xx clients statelessly.
	LegacyModeStateless = "stateless"
	// LegacyModeReject accepts only 2026-07-28+ requests.
	LegacyModeReject = "reject"
)

// RoutableParamHeaderMap maps non-secret SEP-2243 Mcp-Param-<Suffix> suffixes
// (lowercased) to permitted tool argument keys. Secrets MUST NEVER appear here.
var RoutableParamHeaderMap = map[string][]string{
	"argo-instance": {"argoBaseUrl"},
	"kube-context":  {"context"},
	"namespace":     {"namespace"},
	"workflow-name": {"name", "workflowName"},
	"resource-kind": {"kind"},
}

var base64HeaderRe = regexp.MustCompile(`^=\?base64\?([^?]*)\?=$`)
var canonicalBase64Re = regexp.MustCompile(`^[A-Za-z0-9+/]*={0,2}$`)

func decodeMcpHeaderValue(raw string) (string, bool) {
	m := base64HeaderRe.FindStringSubmatch(raw)
	if m == nil {
		return raw, true
	}
	encoded := m[1]
	if len(encoded)%4 != 0 || !canonicalBase64Re.MatchString(encoded) {
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

// HeaderGuard enforces cross-era SEP-2243 anti-desync validation: whenever Mcp-Method,
// Mcp-Name, or Mcp-Param-* headers are present on a POST request, they must exactly match
// the JSON-RPC body (blocking forged outer headers intended to bypass gateway ACLs), while
// permitting header-less 2025-xx requests when legacyMode == "stateless".
func HeaderGuard(legacyMode string, next http.Handler) http.Handler {
	rejectLegacy := strings.EqualFold(strings.TrimSpace(legacyMode), LegacyModeReject)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}

		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, MaxGuardBodyBytes+1))
		_ = r.Body.Close()
		if err != nil {
			writeJSONRPCError(w, http.StatusBadRequest, jsonRPCInvalidRequestCode, "Failed to read request body")
			return
		}
		if len(bodyBytes) > MaxGuardBodyBytes {
			writeJSONRPCError(w, http.StatusRequestEntityTooLarge, jsonRPCInvalidRequestCode, fmt.Sprintf("Request body exceeds %d bytes", MaxGuardBodyBytes))
			return
		}
		// Restore request body for downstream SDK handler.
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		var rpcBody map[string]any
		if err := json.Unmarshal(bodyBytes, &rpcBody); err != nil || rpcBody == nil {
			next.ServeHTTP(w, r)
			return
		}

		rpcID := rpcBody["id"]
		bodyMethod, _ := rpcBody["method"].(string)
		params, _ := rpcBody["params"].(map[string]any)

		if rejectLegacy {
			protoVer := strings.TrimSpace(r.Header.Get("Mcp-Protocol-Version"))
			if bodyMethod == "initialize" || protoVer < "2026-07-28" {
				writeJSONRPCErrorWithID(
					w,
					http.StatusBadRequest,
					jsonRPCInvalidRequestCode,
					"Legacy pre-2026-07-28 MCP requests are disabled (--legacy-mode=reject)",
					rpcID,
				)
				return
			}
		}

		if mcpMethod := r.Header.Get("Mcp-Method"); mcpMethod != "" {
			if strings.TrimSpace(mcpMethod) != bodyMethod {
				writeJSONRPCErrorWithID(
					w,
					http.StatusBadRequest,
					HeaderMismatchErrorCode,
					fmt.Sprintf("Header mismatch: Mcp-Method %q does not match JSON-RPC method %q", mcpMethod, bodyMethod),
					rpcID,
				)
				return
			}
		}

		if mcpName := r.Header.Get("Mcp-Name"); mcpName != "" {
			decoded, ok := decodeMcpHeaderValue(mcpName)
			if !ok {
				writeJSONRPCErrorWithID(w, http.StatusBadRequest, HeaderMismatchErrorCode, "Header mismatch: malformed base64 encoding in Mcp-Name", rpcID)
				return
			}
			bodyName, _ := params["name"].(string)
			if bodyName == "" || decoded != bodyName {
				writeJSONRPCErrorWithID(
					w,
					http.StatusBadRequest,
					HeaderMismatchErrorCode,
					fmt.Sprintf("Header mismatch: Mcp-Name %q does not match JSON-RPC params.name %q", decoded, bodyName),
					rpcID,
				)
				return
			}
		}

		args, _ := params["arguments"].(map[string]any)
		for headerKey, vals := range r.Header {
			lowerKey := strings.ToLower(headerKey)
			if !strings.HasPrefix(lowerKey, "mcp-param-") || len(vals) == 0 {
				continue
			}
			decoded, ok := decodeMcpHeaderValue(vals[0])
			if !ok {
				writeJSONRPCErrorWithID(w, http.StatusBadRequest, HeaderMismatchErrorCode, fmt.Sprintf("Header mismatch: malformed base64 encoding in %s", headerKey), rpcID)
				return
			}
			suffix := strings.TrimPrefix(lowerKey, "mcp-param-")
			candidates, mapped := RoutableParamHeaderMap[suffix]
			if !mapped {
				candidates = []string{suffix}
			}
			if args == nil {
				writeJSONRPCErrorWithID(w, http.StatusBadRequest, HeaderMismatchErrorCode, fmt.Sprintf("Header mismatch: %s supplied but params.arguments is missing", headerKey), rpcID)
				return
			}

			var matchedKey string
			var matchedVal any
			for _, candidate := range candidates {
				if val, exists := args[candidate]; exists {
					matchedKey = candidate
					matchedVal = val
					break
				}
			}
			if matchedKey == "" {
				writeJSONRPCErrorWithID(w, http.StatusBadRequest, HeaderMismatchErrorCode, fmt.Sprintf("Header mismatch: %s does not match any supplied tool argument", headerKey), rpcID)
				return
			}

			normalized, ok := primitiveArgToString(matchedVal)
			if !ok || decoded != normalized {
				writeJSONRPCErrorWithID(
					w,
					http.StatusBadRequest,
					HeaderMismatchErrorCode,
					fmt.Sprintf("Header mismatch: %s value does not match argument %q", headerKey, matchedKey),
					rpcID,
				)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func primitiveArgToString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case bool:
		return strconv.FormatBool(x), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	default:
		return "", false
	}
}

func writeJSONRPCErrorWithID(w http.ResponseWriter, statusCode int, rpcCode int, message string, id any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"error": map[string]any{
			"code":    rpcCode,
			"message": message,
		},
		"id": id,
	})
}
