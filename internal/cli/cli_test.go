package cli

import (
	"bytes"
	"testing"
	"time"
)

func TestRootCommand_HasSubcommands(t *testing.T) {
	cmds := rootCmd.Commands()
	names := make(map[string]bool)
	for _, c := range cmds {
		names[c.Name()] = true
	}
	for _, name := range []string{"version", "pool", "volume", "bucket", "status", "node", "snapshot", "compliance", "heal"} {
		if !names[name] {
			t.Errorf("missing subcommand: %s", name)
		}
	}
}

func TestVersionCommand(t *testing.T) {
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs([]string{"version"})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("version command failed: %v", err)
	}
}

func TestPoolCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range poolCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"list", "get"} {
		if !names[name] {
			t.Errorf("pool missing subcommand: %s", name)
		}
	}
}

func TestVolumeCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range volumeCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"list", "get", "create", "delete"} {
		if !names[name] {
			t.Errorf("volume missing subcommand: %s", name)
		}
	}
}

func TestBucketCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range bucketCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"list", "create", "delete"} {
		if !names[name] {
			t.Errorf("bucket missing subcommand: %s", name)
		}
	}
}

func TestNodeCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range nodeCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"list", "get"} {
		if !names[name] {
			t.Errorf("node missing subcommand: %s", name)
		}
	}
}

func TestSnapshotCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range snapshotCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"list", "get", "create", "delete"} {
		if !names[name] {
			t.Errorf("snapshot missing subcommand: %s", name)
		}
	}
}

func TestComplianceCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range complianceCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"list", "get"} {
		if !names[name] {
			t.Errorf("compliance missing subcommand: %s", name)
		}
	}
}

func TestHealCommand_HasSubcommands(t *testing.T) {
	names := make(map[string]bool)
	for _, c := range healCmd.Commands() {
		names[c.Name()] = true
	}
	for _, name := range []string{"status", "nodes", "volumes"} {
		if !names[name] {
			t.Errorf("heal missing subcommand: %s", name)
		}
	}
}

func TestVolumeCommand_Aliases(t *testing.T) {
	if len(volumeCmd.Aliases) == 0 {
		t.Fatal("volume command should have aliases")
	}
	if volumeCmd.Aliases[0] != "vol" {
		t.Errorf("expected alias 'vol', got %q", volumeCmd.Aliases[0])
	}
}

func TestSnapshotCommand_Aliases(t *testing.T) {
	if len(snapshotCmd.Aliases) == 0 {
		t.Fatal("snapshot command should have aliases")
	}
	if snapshotCmd.Aliases[0] != "snap" {
		t.Errorf("expected alias 'snap', got %q", snapshotCmd.Aliases[0])
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
		{1099511627776, "1.0 TiB"},
		{1536, "1.5 KiB"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := formatBytes(tt.input)
			if got != tt.expected {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"less than second", 500 * time.Millisecond, "< 1s"},
		{"seconds", 30 * time.Second, "30s"},
		{"minutes", 5 * time.Minute, "5m"},
		{"hours", 2 * time.Hour, "2.0h"},
		{"days", 48 * time.Hour, "2.0d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatDuration(tt.d); got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}
