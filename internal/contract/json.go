package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

// DecodeStrict rejects unknown/case-variant keys, missing required properties,
// null scalars/collections, and invalid array element types before Go decoding.
// Fields with omitempty may be absent. Pointer fields may explicitly be null.
func DecodeStrict(data []byte, out any) error { return decodeStrictBounded(data, out, 1<<20, 16384) }

// DecodeStrictBounded permits a larger trusted host request without raising
// the default one-MiB/16K-character bounds used by model output and tools.
// Host strings may span this envelope; the host still validates field semantics.
func DecodeStrictBounded(data []byte, out any, limit int) error {
	return decodeStrictBounded(data, out, limit, limit)
}

func decodeStrictBounded(data []byte, out any, limit, maxStringRunes int) error {
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("output must be nonnil pointer")
	}
	if limit < 1 || limit > 16<<20 || len(data) > limit || !utf8.Valid(data) || !json.Valid(data) {
		return fmt.Errorf("invalid JSON envelope")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := uniqueJSONKeys(decoder); err != nil {
		return err
	}
	if err := validateJSON(bytes.TrimSpace(data), v.Elem().Type(), "$", maxStringRunes); err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func uniqueJSONKeys(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid JSON property")
			}
			seen[name] = true
			if err = uniqueJSONKeys(d); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	case json.Delim('['):
		for d.More() {
			if err := uniqueJSONKeys(d); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	}
	return nil
}
func validateJSON(raw json.RawMessage, t reflect.Type, path string, maxStringRunes int) error {
	if t.Kind() == reflect.Pointer {
		if bytes.Equal(raw, []byte("null")) {
			return nil
		}
		return validateJSON(raw, t.Elem(), path, maxStringRunes)
	}
	if bytes.Equal(raw, []byte("null")) {
		return fmt.Errorf("%s: null not allowed", path)
	}
	if t == reflect.TypeFor[time.Time]() {
		if err := validateJSON(raw, reflect.TypeFor[string](), path, maxStringRunes); err != nil {
			return err
		}
		var timestamp time.Time
		if err := json.Unmarshal(raw, &timestamp); err != nil {
			return fmt.Errorf("%s: invalid timestamp: %w", path, err)
		}
		return nil
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
			if err := validateJSON(bytes.TrimSpace(value), f.Type, path+"."+name, maxStringRunes); err != nil {
				return err
			}
		}
		for key := range fields {
			if !known[key] {
				return fmt.Errorf("%s.%s: unknown field", path, key)
			}
		}
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return fmt.Errorf("unsupported map key")
		}
		var values map[string]json.RawMessage
		if len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, &values) != nil {
			return fmt.Errorf("%s: expected map", path)
		}
		if len(values) > 256 {
			return fmt.Errorf("%s: too many map entries", path)
		}
		for key, value := range values {
			if err := validateJSON(bytes.TrimSpace(value), t.Elem(), path+"."+key, maxStringRunes); err != nil {
				return err
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
			if err := validateJSON(bytes.TrimSpace(value), t.Elem(), fmt.Sprintf("%s[%d]", path, i), maxStringRunes); err != nil {
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
		if utf8.RuneCountInString(value) > maxStringRunes {
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
	if t == reflect.TypeFor[time.Time]() {
		return map[string]any{"type": "string", "format": "date-time", "maxLength": 16384}
	}
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
