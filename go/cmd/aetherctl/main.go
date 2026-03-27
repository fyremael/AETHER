package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fyremael/aether/go/internal/client"
	opstui "github.com/fyremael/aether/go/internal/tui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	return runWithArgs(os.Args[1:], os.Getenv, os.ReadFile)
}

func runWithArgs(
	args []string,
	getenv func(string) string,
	readFile func(string) ([]byte, error),
) error {
	root := flag.NewFlagSet("aetherctl", flag.ContinueOnError)
	root.SetOutput(ioDiscard{})

	baseURL := root.String("base-url", "http://127.0.0.1:3000", "AETHER HTTP base URL")
	tokenFlag := root.String("token", "", "Bearer token for authenticated endpoints")
	tokenFile := root.String("token-file", "", "Path to a bearer token file for authenticated endpoints")

	if err := root.Parse(args); err != nil {
		return err
	}
	if len(root.Args()) == 0 {
		return usageError("missing command")
	}

	token, err := resolveBearerToken(*tokenFlag, *tokenFile, getenv, readFile)
	if err != nil {
		return err
	}

	commandName := root.Args()[0]
	commandArgs := root.Args()[1:]
	api := client.New(*baseURL, token)
	ctx := context.Background()

	switch commandName {
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
		if err := command.Parse(commandArgs); err != nil {
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
		capabilities := command.String("capabilities", "", "Comma-separated capabilities")
		visibilities := command.String("visibilities", "", "Comma-separated visibilities")
		if err := command.Parse(commandArgs); err != nil {
			return err
		}
		if *tupleID == 0 {
			return usageError("explain requires --tuple-id")
		}
		response, err := api.ExplainTupleWithPolicy(
			ctx,
			*tupleID,
			buildPolicyContext(*capabilities, *visibilities),
		)
		if err != nil {
			return err
		}
		return printJSON(response)
	case "tui":
		command := flag.NewFlagSet("tui", flag.ContinueOnError)
		command.SetOutput(ioDiscard{})
		capabilities := command.String("capabilities", "", "Comma-separated capabilities")
		visibilities := command.String("visibilities", "", "Comma-separated visibilities")
		refresh := command.Duration("refresh", 2*time.Second, "Refresh interval for live tabs")
		if err := command.Parse(commandArgs); err != nil {
			return err
		}
		if strings.TrimSpace(token) == "" {
			return usageError("tui requires --token, --token-file, or AETHER_TOKEN")
		}
		return opstui.Run(api, *baseURL, buildPolicyContext(*capabilities, *visibilities), *refresh)
	default:
		return usageError("unknown command: " + commandName)
	}
}

func resolveBearerToken(
	tokenFlag string,
	tokenFile string,
	getenv func(string) string,
	readFile func(string) ([]byte, error),
) (string, error) {
	tokenFlag = strings.TrimSpace(tokenFlag)
	tokenFile = strings.TrimSpace(tokenFile)
	if tokenFlag != "" && tokenFile != "" {
		return "", fmt.Errorf("use either --token or --token-file, not both")
	}
	if tokenFlag != "" {
		return tokenFlag, nil
	}
	if tokenFile != "" {
		content, err := readFile(tokenFile)
		if err != nil {
			return "", err
		}
		token := strings.TrimSpace(string(content))
		if token == "" {
			return "", fmt.Errorf("token file %s is empty", tokenFile)
		}
		return token, nil
	}
	return strings.TrimSpace(getenv("AETHER_TOKEN")), nil
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
	return fmt.Errorf("%s\nusage: aetherctl [-base-url URL] [-token TOKEN | -token-file PATH] <health|history|run|explain|tui> [flags]", message)
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
