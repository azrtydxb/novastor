package cli

import (
	"bytes"
	"testing"
)

func TestRootCommand_HasSubcommands(t *testing.T) {
	cmds := rootCmd.Commands()
	names := make(map[string]bool)
	for _, c := range cmds {
		names[c.Name()] = true
	}
	for _, name := range []string{"version", "pool", "volume", "bucket", "status"} {
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

func TestVolumeCommand_Aliases(t *testing.T) {
	if len(volumeCmd.Aliases) == 0 {
		t.Fatal("volume command should have aliases")
	}
	if volumeCmd.Aliases[0] != "vol" {
		t.Errorf("expected alias 'vol', got %q", volumeCmd.Aliases[0])
	}
}
