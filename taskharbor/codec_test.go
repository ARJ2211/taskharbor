package taskharbor

import (
	"reflect"
	"testing"
)

func TestJsonCodec_RoundTrip(t *testing.T) {
	type Payload struct {
		Name  string
		Count int
	}

	var c Codec = JsonCodec{}

	in := Payload{Name: "aayush", Count: 7}

	b, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var out Payload
	err = c.Unmarshal(b, &out)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !reflect.DeepEqual(in, out) {
		t.Fatalf("expected %+v, got %+v", in, out)
	}
}
