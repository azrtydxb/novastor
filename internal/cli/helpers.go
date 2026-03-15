package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connectMeta creates a gRPC connection to the metadata service using the
// address from the --meta-addr flag. When TLS flags are provided, mTLS is used.
func connectMeta() (*metadata.GRPCClient, error) {
	var opts []grpc.DialOption
	if tlsCA != "" && tlsCert != "" && tlsKey != "" {
		tlsOpt, err := transport.NewClientTLS(transport.TLSConfig{
			CACertPath: tlsCA,
			CertPath:   tlsCert,
			KeyPath:    tlsKey,
		})
		if err != nil {
			return nil, fmt.Errorf("configuring TLS: %w", err)
		}
		opts = append(opts, tlsOpt)
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return metadata.Dial(metaAddr, opts...)
}

// printTable renders headers and rows as a neatly aligned table using tabwriter.
func printTable(headers []string, rows [][]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	w.Flush()
}

// printTableOrJSON prints a table or JSON output based on the output flag.
// For JSON, the data should be a slice of maps with keys matching the headers.
func printTableOrJSON(headers []string, rows [][]string, jsonData any) {
	if output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(jsonData); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
			os.Exit(1)
		}
		return
	}
	printTable(headers, rows)
}

// formatBytes converts a byte count into a human-readable string using
// binary units (KiB, MiB, GiB, TiB).
func formatBytes(b int64) string {
	const (
		kib = 1024
		mib = kib * 1024
		gib = mib * 1024
		tib = gib * 1024
	)
	switch {
	case b >= tib:
		return fmt.Sprintf("%.1f TiB", float64(b)/float64(tib))
	case b >= gib:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(gib))
	case b >= mib:
		return fmt.Sprintf("%.1f MiB", float64(b)/float64(mib))
	case b >= kib:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(kib))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
