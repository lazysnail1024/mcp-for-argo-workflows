package governance

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultMaxLogBytes hard-caps log content returned to LLM context windows (32KB).
	DefaultMaxLogBytes = 32 * 1024

	untrustedLogCloseTag = "</untrusted_workflow_log>"
)

// ansiEscapePattern strips ANSI/VT100 terminal escape codes from container logs.
var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)`)

// CleanAndGuardLogs strips ANSI sequences, collapses carriage-return progress-bar spam,
// scrubs secrets, trims safely to maxBytes (keeping the most recent tail lines), and wraps
// the result in explicit spotlighting delimiters against indirect prompt injection.
func CleanAndGuardLogs(raw, podName, container string, maxBytes int) (wrapped string, truncated bool) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxLogBytes
	}

	cleaned := CleanLogText(raw)
	if len(cleaned) > maxBytes {
		cleaned = "... (earlier log output omitted to protect context budget)\n" + trimTailUTF8(cleaned, maxBytes)
		truncated = true
	}

	// Prevent log content from closing the untrusted spotlighting envelope prematurely.
	cleaned = strings.ReplaceAll(cleaned, untrustedLogCloseTag, "&lt;/untrusted_workflow_log&gt;")

	safePod := sanitizeAttrValue(podName)
	safeContainer := sanitizeAttrValue(container)
	wrapped = fmt.Sprintf(
		"<untrusted_workflow_log pod=%q container=%q>\n%s\n%s",
		safePod, safeContainer, cleaned, untrustedLogCloseTag,
	)
	return wrapped, truncated
}

// CleanLogText sanitizes raw pod log text without adding the XML spotlighting wrapper.
func CleanLogText(raw string) string {
	if raw == "" {
		return ""
	}
	// 1. Strip ANSI terminal escape sequences.
	s := ansiEscapePattern.ReplaceAllString(raw, "")

	// 2. Process line by line: collapse \r progress bars (e.g. tqdm / curl) and control chars.
	rawLines := strings.Split(s, "\n")
	lines := make([]string, 0, len(rawLines))
	var prevLine string
	repeatCount := 0

	flushRepeats := func() {
		if repeatCount > 0 {
			lines = append(lines, fmt.Sprintf("... (previous line repeated %d more time(s))", repeatCount))
			repeatCount = 0
		}
	}

	for _, line := range rawLines {
		// Handle CR-based progress redraws: keep the last non-empty segment after '\r'.
		if strings.ContainsRune(line, '\r') {
			parts := strings.Split(line, "\r")
			resolved := ""
			for i := len(parts) - 1; i >= 0; i-- {
				if strings.TrimSpace(parts[i]) != "" {
					resolved = parts[i]
					break
				}
			}
			line = resolved
		}

		// Strip non-printable control characters except horizontal tab.
		line = stripControlChars(line)
		line = RedactSecretsInText(line)

		if len(lines) > 0 && line == prevLine && strings.TrimSpace(line) != "" {
			repeatCount++
			continue
		}
		flushRepeats()
		lines = append(lines, line)
		prevLine = line
	}
	flushRepeats()

	return strings.TrimRight(strings.Join(lines, "\n"), "\n")
}

// GuardUntrustedSnippet wraps short untrusted external text (such as a node error message
// from user code) after scrubbing secrets and control characters.
func GuardUntrustedSnippet(text string, maxRunes int) string {
	cleaned := RedactSecretsInText(stripControlChars(ansiEscapePattern.ReplaceAllString(text, "")))
	if maxRunes <= 0 {
		return cleaned
	}
	runes := []rune(cleaned)
	if len(runes) <= maxRunes {
		return cleaned
	}
	return string(runes[:maxRunes]) + "..."
}

func stripControlChars(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' {
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

func sanitizeAttrValue(v string) string {
	v = stripControlChars(v)
	v = strings.ReplaceAll(v, "<", "")
	v = strings.ReplaceAll(v, ">", "")
	return strings.TrimSpace(v)
}

// trimTailUTF8 keeps the final maxBytes of s aligned cleanly on a UTF-8 rune boundary
// and prefers starting at the next newline for readability.
func trimTailUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	start := len(s) - maxBytes
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	tail := s[start:]
	if idx := strings.IndexByte(tail, '\n'); idx >= 0 && idx < len(tail)-1 {
		return tail[idx+1:]
	}
	return tail
}
