package cli

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/piwi3910/novastor/internal/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connectMeta creates a gRPC connection to the metadata service using the
// address from the --meta-addr flag.
func connectMeta() (*metadata.GRPCClient, error) {
	return metadata.Dial(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
