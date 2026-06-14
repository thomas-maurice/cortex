// Package config locates and loads the optional cortex.yaml shared by the CLI
// and the MCP server. The file provides defaults for settings that are otherwise
// taken from flags/env; the resolution order callers build on top of this is:
// explicit flag > environment variable > config file > built-in default.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Dir is the directory cortex searches for cortex.yaml. XDG_CONFIG_HOME wins when
// set; otherwise we pin ~/.config/cortex on every platform (rather than macOS's
// ~/Library/Application Support) so the path is predictable across hosts.
func Dir() string {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "cortex")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "cortex")
}

// FilePath is the default config file location, ~/.config/cortex/cortex.yaml.
func FilePath() string { return filepath.Join(Dir(), "cortex.yaml") }

// New returns a viper instance with cortex.yaml loaded. When file is non-empty it
// overrides the default search path. A missing file is not an error — the config
// is optional — but malformed YAML, or an explicitly-passed unreadable path, is.
// Callers layer flags/env/defaults on top via BindPFlag / BindEnv / SetDefault.
func New(file string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	if file != "" {
		v.SetConfigFile(file)
	} else {
		v.SetConfigName("cortex")
		v.AddConfigPath(Dir())
	}
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read config %s: %w", v.ConfigFileUsed(), err)
		}
	}
	return v, nil
}
