package whale

import (
	"bytes"

	"gopkg.in/yaml.v3"
)

// FormatConfigYAML renders cfg as YAML suitable for config/whale.yaml.
func FormatConfigYAML(cfg Config) string {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	_ = enc.Encode(cfg)
	return buf.String()
}
