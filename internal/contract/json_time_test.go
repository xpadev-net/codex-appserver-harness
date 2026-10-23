package contract

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStrictTimestampRoundTripAndStructuralRejection(t *testing.T) {
	type request struct {
		CompletedAt time.Time `json:"completed_at"`
	}
	for _, timestamp := range []time.Time{{}, time.Date(2026, 10, 23, 15, 0, 0, 123456789, time.FixedZone("local", 9*3600))} {
		raw, err := json.Marshal(request{timestamp})
		if err != nil {
			t.Fatal(err)
		}
		var got request
		if err = DecodeStrict(raw, &got); err != nil || !got.CompletedAt.Equal(timestamp) {
			t.Fatal(string(raw), got, err)
		}
	}
	for _, raw := range []string{`{}`, `{"completed_at":null}`, `{"completed_at":{}}`, `{"completed_at":123}`, `{"completed_at":"not a timestamp"}`, `{"Completed_At":"2026-10-23T00:00:00Z"}`, `{"completed_at":"2026-10-23T00:00:00Z","extra":true}`, `{"completed_at":"2026-10-23T00:00:00Z","completed_at":"2026-10-23T00:00:00Z"}`} {
		var got request
		if err := DecodeStrict([]byte(raw), &got); err == nil {
			t.Fatal("malformed timestamp request accepted", raw)
		}
	}
	schema := Schema(request{})
	property := schema["properties"].(map[string]any)["completed_at"].(map[string]any)
	if property["type"] != "string" || property["format"] != "date-time" {
		t.Fatal(property)
	}
}
