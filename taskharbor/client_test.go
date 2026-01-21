package taskharbor

import (
	"context"
	"testing"
	"time"

	"github.com/ARJ2211/taskharbor/taskharbor/driver/memory"
)

func TestClient_EnqueueStoresJob(t *testing.T) {
	ctx := context.Background()

	d := memory.New()
	c := NewClient(d)

	type Payload struct {
		X int
	}

	id, err := c.Enqueue(ctx, JobRequest{
		Type:    "test.job",
		Payload: Payload{X: 7},
		Queue:   "default",
		RunAt:   time.Time{},
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	now := time.Now().UTC()
	rec, ok, err := d.Reserve(ctx, "default", now)
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true, got ok=false")
	}
	if rec.ID != string(id) {
		t.Fatalf("expected id %s, got %s", string(id), rec.ID)
	}
	if rec.Type != "test.job" {
		t.Fatalf("expected type test.job, got %s", rec.Type)
	}
	if len(rec.Payload) == 0 {
		t.Fatalf("expected payload bytes, got empty")
	}
}
