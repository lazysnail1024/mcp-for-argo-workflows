package server

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RunStdio runs the MCP server over standard input/output for local CLI/IDE integrations.
func (s *Server) RunStdio(ctx context.Context) error {
	return s.mcp.Run(ctx, &mcp.StdioTransport{})
}
