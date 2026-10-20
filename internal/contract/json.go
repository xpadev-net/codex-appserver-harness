package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"
)

// DecodeStrict rejects unknown/case-variant keys, missing required properties,
// null scalars/collections, and invalid array element types before Go decoding.
// Fields with omitempty may be absent. Pointer fields may explicitly be null.
func DecodeStrict(data []byte, out any) error {
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("output must be nonnil pointer")
	}
	if len(data) > 1<<20 || !json.Valid(data) {
		return fmt.Errorf("invalid JSON envelope")
	}
	if err := validateJSON(bytes.TrimSpace(data), v.Elem().Type(), "$"); err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}
func validateJSON(raw json.RawMessage, t reflect.Type, path string) error {
	if t.Kind() == reflect.Pointer {
		if bytes.Equal(raw, []byte("null")) {
			return nil
		}
		return validateJSON(raw, t.Elem(), path)
	}
	if bytes.Equal(raw, []byte("null")) {
		return fmt.Errorf("%s: null not allowed", path)
	}
	switch t.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, &fields) != nil {
			return fmt.Errorf("%s: expected object", path)
		}
		known := map[string]bool{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := strings.Split(f.Tag.Get("json"), ",")
			name := tag[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			known[name] = true
			value, ok := fields[name]
			optional := len(tag) > 1 && tag[1] == "omitempty"
			if !ok {
				if optional {
					continue
				}
				return fmt.Errorf("%s.%s: required", path, name)
			}
			if err := validateJSON(bytes.TrimSpace(value), f.Type, path+"."+name); err != nil {
				return err
			}
		}
		for key := range fields {
			if !known[key] {
				return fmt.Errorf("%s.%s: unknown field", path, key)
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if len(raw) == 0 || raw[0] != '[' || json.Unmarshal(raw, &values) != nil {
			return fmt.Errorf("%s: expected array", path)
		}
		if len(values) > 256 {
			return fmt.Errorf("%s: too many items", path)
		}
		for i, value := range values {
			if err := validateJSON(bytes.TrimSpace(value), t.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case reflect.String:
		if len(raw) == 0 || raw[0] != '"' {
			return fmt.Errorf("%s: expected string", path)
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		if utf8.RuneCountInString(value) > 16384 {
			return fmt.Errorf("%s: string too long", path)
		}
	case reflect.Int, reflect.Int64:
		var value int64
		if json.Unmarshal(raw, &value) != nil {
			return fmt.Errorf("%s: expected integer", path)
		}
	case reflect.Bool:
		if !bytes.Equal(raw, []byte("true")) && !bytes.Equal(raw, []byte("false")) {
			return fmt.Errorf("%s: expected boolean", path)
		}
	default:
		return fmt.Errorf("unsupported schema type %s", t)
	}
	return nil
}

// Schema mirrors DecodeStrict's structural contract for app-server outputSchema.
// Domain-specific identities, allowed references, and outcomes are checked separately.
func Schema(value any) map[string]any {
	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return typeSchema(t)
}
func typeSchema(t reflect.Type) map[string]any {
	switch t.Kind() {
	case reflect.Struct:
		properties := map[string]any{}
		required := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := strings.Split(f.Tag.Get("json"), ",")
			name := tag[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			property := typeSchema(f.Type)
			if name == "schema_version" {
				property["const"] = 1
			}
			properties[name] = property
			if len(tag) == 1 || tag[1] != "omitempty" {
				required = append(required, name)
			}
		}
		return map[string]any{"type": "object", "additionalProperties": false, "properties": properties, "required": required}
	case reflect.Slice:
		return map[string]any{"type": "array", "items": typeSchema(t.Elem()), "maxItems": 256}
	case reflect.String:
		return map[string]any{"type": "string", "maxLength": 16384}
	case reflect.Int, reflect.Int64:
		return map[string]any{"type": "integer"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Pointer:
		return map[string]any{"anyOf": []any{typeSchema(t.Elem()), map[string]string{"type": "null"}}}
	}
	return map[string]any{}
}
