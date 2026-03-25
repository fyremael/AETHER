package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/fyremael/aether/go/internal/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	baseURL := flag.String("base-url", "http://127.0.0.1:3000", "AETHER HTTP base URL")
	token := flag.String("token", "", "Bearer token for authenticated endpoints")
	flag.Parse()

	if flag.NArg() == 0 {
		return usageError("missing command")
	}

	api := client.New(*baseURL, *token)
	ctx := context.Background()

	switch flag.Arg(0) {
	case "health":
		response, err := api.Health(ctx)
		if err != nil {
			return err
		}
		return printJSON(response)
	case "history":
		response, err := api.History(ctx)
		if err != nil {
			return err
		}
		return printJSON(response)
	case "run":
		command := flag.NewFlagSet("run", flag.ContinueOnError)
		command.SetOutput(ioDiscard{})
		dslFile := command.String("file", "", "Path to an AETHER DSL document")
		capabilities := command.String("capabilities", "", "Comma-separated capabilities")
		visibilities := command.String("visibilities", "", "Comma-separated visibilities")
		if err := command.Parse(flag.Args()[1:]); err != nil {
			return err
		}
		if *dslFile == "" {
			return usageError("run requires --file")
		}
		dsl, err := os.ReadFile(*dslFile)
		if err != nil {
			return err
		}
		request := client.RunDocumentRequest{DSL: string(dsl)}
		if context := buildPolicyContext(*capabilities, *visibilities); context != nil {
			request.PolicyContext = context
		}
		response, err := api.RunDocument(ctx, request)
		if err != nil {
			return err
		}
		return printJSON(response)
	case "explain":
		command := flag.NewFlagSet("explain", flag.ContinueOnError)
		command.SetOutput(ioDiscard{})
		tupleID := command.Uint64("tuple-id", 0, "Tuple ID to explain")
		if err := command.Parse(flag.Args()[1:]); err != nil {
			return err
		}
		if *tupleID == 0 {
			return usageError("explain requires --tuple-id")
		}
		response, err := api.ExplainTuple(ctx, *tupleID)
		if err != nil {
			return err
		}
		return printJSON(response)
	default:
		return usageError("unknown command: " + flag.Arg(0))
	}
}

func buildPolicyContext(capabilities string, visibilities string) *client.PolicyContext {
	context := &client.PolicyContext{
		Capabilities: splitCSV(capabilities),
		Visibilities: splitCSV(visibilities),
	}
	if len(context.Capabilities) == 0 && len(context.Visibilities) == 0 {
		return nil
	}
	return context
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	rawParts := strings.Split(value, ",")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		trimmed := strings.TrimSpace(raw)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func usageError(message string) error {
	return fmt.Errorf("%s\nusage: aetherctl [-base-url URL] [-token TOKEN] <health|history|run|explain> [flags]", message)
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
