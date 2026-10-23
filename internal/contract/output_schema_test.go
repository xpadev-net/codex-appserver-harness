package contract

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func assertOutputPropertiesRequired(t *testing.T, node map[string]any, path string) {
	t.Helper()
	if properties, ok := node["properties"].(map[string]any); ok {
		expected := []string{}
		for name := range properties {
			expected = append(expected, name)
		}
		sort.Strings(expected)
		if !reflect.DeepEqual(expected, node["required"]) {
			t.Fatalf("%s: output properties must all be required: %#v != %#v", path, expected, node["required"])
		}
		if node["additionalProperties"] != false {
			t.Fatalf("%s: unbounded output object", path)
		}
		for name, child := range properties {
			if child, ok := child.(map[string]any); ok {
				assertOutputPropertiesRequired(t, child, path+"."+name)
			}
		}
	}
	if child, ok := node["items"].(map[string]any); ok {
		assertOutputPropertiesRequired(t, child, path+"[]")
	}
	for _, key := range []string{"anyOf", "oneOf", "allOf"} {
		if children, ok := node[key].([]any); ok {
			for _, child := range children {
				if child, ok := child.(map[string]any); ok {
					assertOutputPropertiesRequired(t, child, path+"."+key)
				}
			}
		}
	}
}
func TestNativeOutputSchemasRequireAllPropertiesRecursively(t *testing.T) {
	for name, schema := range map[string]map[string]any{"main": MainResultSchema(), "task": TaskResultSchema()} {
		before, _ := json.Marshal(schema)
		output := StrictOutputSchema(schema)
		assertOutputPropertiesRequired(t, output, name)
		after, _ := json.Marshal(schema)
		if string(before) != string(after) {
			t.Fatal("provider adaptation mutated original input contract", name)
		}
	}
}
func TestNativeOutputSchemaPreservesNullableAndOptionalInputSemantics(t *testing.T) {
	type object struct {
		Required   string `json:"required"`
		Properties *struct {
			Optional string `json:"optional,omitempty"`
		} `json:"properties,omitempty"`
		Values []string `json:"values,omitempty"`
	}
	generic := Schema(object{})
	output := StrictOutputSchema(generic)
	assertOutputPropertiesRequired(t, output, "root")
	var decoded object
	if err := DecodeStrict([]byte(`{"required":"input may omit optional fields"}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if err := DecodeStrict([]byte(`{"required":"output includes null/empty","properties":null,"values":[]}`), &decoded); err != nil {
		t.Fatal(err)
	}
	fields := output["properties"].(map[string]any)
	alternatives := fields["properties"].(map[string]any)["anyOf"].([]any)
	if alternatives[1].(map[string]string)["type"] != "null" || fields["values"].(map[string]any)["type"] != "array" {
		t.Fatal("optional output type changed", fields)
	}
}
