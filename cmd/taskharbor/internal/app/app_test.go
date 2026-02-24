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

func TestListAndInspectAfterEnqueue(t *testing.T) {
	var out1, err1 bytes.Buffer
	code := Run([]string{"enqueue", "--type", "echo", "--payload", "hi"}, &out1, &err1)
	if code != 0 {
		t.Fatalf("enqueue expected 0, got %d (stderr=%q)", code, err1.String())
	}
	id := strings.TrimSpace(out1.String())
	if id == "" {
		t.Fatalf("expected job id")
	}

	var out2, err2 bytes.Buffer
	code = Run([]string{"list"}, &out2, &err2)
	if code != 0 {
		t.Fatalf("list expected 0, got %d (stderr=%q)", code, err2.String())
	}
	if !strings.Contains(out2.String(), id) {
		t.Fatalf("expected list to contain id %q, got: %q", id, out2.String())
	}

	var out3, err3 bytes.Buffer
	code = Run([]string{"inspect", id}, &out3, &err3)
	if code != 0 {
		t.Fatalf("inspect expected 0, got %d (stderr=%q)", code, err3.String())
	}
	if !strings.Contains(out3.String(), "id: "+id) {
		t.Fatalf("expected inspect output to include id, got: %q", out3.String())
	}
}
