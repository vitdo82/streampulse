// Package config provides configuration loading via viper.
package config

// Config holds all StreamPulse configuration.
type Config struct {
	Brokers        []string      `mapstructure:"brokers"`
	ScrapeInterval string        `mapstructure:"scrape_interval"`
	Storage        StorageConfig `mapstructure:"storage"`
	Alerts         []AlertRule   `mapstructure:"alerts"`
}

// StorageConfig holds the storage backend configuration.
type StorageConfig struct {
	Type     string       `mapstructure:"type"`
	SQLite   SQLiteConfig `mapstructure:"sqlite"`
	Postgres DBConfig     `mapstructure:"postgres"`
}

// SQLiteConfig holds SQLite-specific settings.
type SQLiteConfig struct {
	Path string `mapstructure:"path"`
}

// DBConfig holds database connection settings.
type DBConfig struct {
	DSN      string `mapstructure:"dsn"`
	MaxConns int    `mapstructure:"max_connections"`
	SSLMode  string `mapstructure:"ssl_mode"`
}

// AlertRule defines an alert condition.
type AlertRule struct {
	Name      string         `mapstructure:"name"`
	Group     string         `mapstructure:"group"`
	Condition string         `mapstructure:"condition"`
	For       string         `mapstructure:"for"`
	Severity  string         `mapstructure:"severity"`
	Notify    []AlertChannel `mapstructure:"notify"`
}

// AlertChannel defines a notification target.
type AlertChannel struct {
	Type    string `mapstructure:"type"`
	Webhook string `mapstructure:"webhook"`
	Channel string `mapstructure:"channel"`
	To      string `mapstructure:"to"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ScrapeInterval: "5s",
		Storage: StorageConfig{
			Type: "sqlite",
			SQLite: SQLiteConfig{
				Path: "~/.streampulse/state.db",
			},
		},
	}
}
