package contract

import "sort"

// StrictOutputSchema adapts a fresh copy for native structured output: every
// declared object property is required. Optional pointers retain their nullable
// alternatives; optional arrays/scalars retain their ordinary zero-value types.
// Input/tool schemas and DecodeStrict's omission rules remain unchanged.
func StrictOutputSchema(schema map[string]any) map[string]any {
	out := copySchema(schema).(map[string]any)
	requireOutputProperties(out)
	return out
}
func copySchema(value any) any {
	switch value := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, child := range value {
			out[key] = copySchema(child)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(value))
		for key, child := range value {
			out[key] = child
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, child := range value {
			out[i] = copySchema(child)
		}
		return out
	case []string:
		return append([]string{}, value...)
	default:
		return value
	}
}
func requireOutputProperties(schema map[string]any) {
	if properties, ok := schema["properties"].(map[string]any); ok {
		required := make([]string, 0, len(properties))
		for name, property := range properties {
			required = append(required, name)
			if child, ok := property.(map[string]any); ok {
				requireOutputProperties(child)
			}
		}
		sort.Strings(required)
		schema["required"] = required
	}
	for _, name := range []string{"items", "additionalProperties"} {
		if child, ok := schema[name].(map[string]any); ok {
			requireOutputProperties(child)
		}
	}
	for _, name := range []string{"anyOf", "oneOf", "allOf"} {
		if children, ok := schema[name].([]any); ok {
			for _, value := range children {
				if child, ok := value.(map[string]any); ok {
					requireOutputProperties(child)
				}
			}
		}
	}
}
