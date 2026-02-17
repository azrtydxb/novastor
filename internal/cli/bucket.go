package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/spf13/cobra"
)

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Manage S3 buckets",
}

var bucketListCmd = &cobra.Command{
	Use:   "list",
	Short: "List S3 buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		buckets, err := client.ListBucketMetas(ctx)
		if err != nil {
			return fmt.Errorf("listing buckets: %w", err)
		}

		headers := []string{"NAME", "OWNER", "CREATED"}
		var rows [][]string
		for _, b := range buckets {
			created := time.Unix(0, b.CreationDate).UTC().Format("2006-01-02T15:04:05Z")
			rows = append(rows, []string{b.Name, b.Owner, created})
		}
		printTable(headers, rows)
		return nil
	},
}

var bucketCreateCmd = &cobra.Command{
	Use:   "create [name]",
	Short: "Create an S3 bucket",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		meta := &metadata.BucketMeta{
			Name:         args[0],
			Owner:        "admin",
			CreationDate: time.Now().UnixNano(),
		}

		ctx := context.Background()
		if err := client.PutBucketMeta(ctx, meta); err != nil {
			return fmt.Errorf("creating bucket: %w", err)
		}

		fmt.Printf("Bucket created: %s\n", args[0])
		return nil
	},
}

var bucketDeleteCmd = &cobra.Command{
	Use:   "delete [name]",
	Short: "Delete an S3 bucket",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := connectMeta()
		if err != nil {
			return err
		}
		defer client.Close()

		ctx := context.Background()
		if err := client.DeleteBucketMeta(ctx, args[0]); err != nil {
			return fmt.Errorf("deleting bucket %q: %w", args[0], err)
		}

		fmt.Printf("Bucket deleted: %s\n", args[0])
		return nil
	},
}

func init() {
	bucketCmd.AddCommand(bucketListCmd, bucketCreateCmd, bucketDeleteCmd)
	rootCmd.AddCommand(bucketCmd)
}
