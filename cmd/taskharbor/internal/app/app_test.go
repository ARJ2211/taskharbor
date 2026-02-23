package app

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootHelp(t *testing.T) {
	var out, err bytes.Buffer
	code := Run([]string{"--help"}, &out, &err)
	if code != 0 {
		t.Fatalf("expected 0, got %d (stderr=%q)", code, err.String())
	}
	if !strings.Contains(out.String(), "worker run") {
		t.Fatalf("expected help to mention worker run, got: %q", out.String())
	}
}

func TestWorkerRunHelp(t *testing.T) {
	var out, err bytes.Buffer
	code := Run([]string{"worker", "run", "--help"}, &out, &err)
	if code != 0 {
		t.Fatalf("expected 0, got %d (stderr=%q)", code, err.String())
	}
	if !strings.Contains(out.String(), "worker run") {
		t.Fatalf("expected worker run usage, got: %q", out.String())
	}
}

func TestUnknownCommand(t *testing.T) {
	var out, err bytes.Buffer
	code := Run([]string{"nope"}, &out, &err)
	if code == 0 {
		t.Fatalf("expected non-zero, got %d", code)
	}
}

func TestEnqueueMinimal(t *testing.T) {
	var out, err bytes.Buffer
	code := Run([]string{"enqueue", "--type", "echo", "--payload", "hi"}, &out, &err)
	if code != 0 {
		t.Fatalf("expected 0, got %d (stderr=%q)", code, err.String())
	}
	if strings.TrimSpace(out.String()) == "" {
		t.Fatalf("expected a job id, got empty output")
	}
}
