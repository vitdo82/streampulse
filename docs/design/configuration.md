# Design: Configuration (viper)

**Status:** Design · **Depends on:** nothing · **Serves:** all other features

## Goal

Replace the dead `internal/config/config.go` struct with a fully wired configuration stack: defaults → YAML file → environment variables → CLI flags. Every command (`serve`, `check`, `dlq`, `analyze`, `alerts`, root TUI) loads config through one path.

## Sources & precedence

```
defaults < config file < environment < flags
```

1. **Defaults** — `DefaultConfig()` (already exists, `config.go:50`).
2. **Config file** — first existing of:
   - `--config <path>` flag
   - `$STREAMPULSE_CONFIG`
   - `~/.config/streampulse/streampulse.yaml`
   - `/etc/streampulse/streampulse.yaml`
3. **Environment** — prefix `STREAMPULSE_`, nested keys via `_` (e.g. `STREAMPULSE_STORAGE_SQLITE_PATH`). Viper's `AutomaticEnv` + `SetEnvKeyReplacer(strings.NewReplacer(".", "_"))`.
4. **Flags** — `--brokers` already exists on the root command; new flags per command.

## Config struct (extended)

```yaml
# streampulse.yaml
brokers: ["localhost:9093"]
scrape_interval: 5s
cluster_id: local-dev

storage:
  type: sqlite            # sqlite | postgres | clickhouse
  sqlite:
    path: ~/.streampulse/state.db
  postgres:
    dsn: ""
    max_connections: 10
    ssl_mode: disable

kafka:
  tls:
    enabled: false
    ca_file: ""
    cert_file: ""          # mTLS
    key_file: ""           # mTLS
    insecure_skip_verify: false
  sasl:
    mechanism: ""          # plain | scram-sha-256 | scram-sha-512
    username: ""
    password_env: STREAMPULSE_SASL_PASSWORD   # never in yaml

alerts:
  - name: consumer-lag
    group: consumers
    condition: "lag > 1000"
    for: 2m
    severity: warning
    notify:
      - type: slack
        webhook_env: SLACK_WEBHOOK_URL
      - type: email
        to: ["oncall@example.com"]

prometheus:
  listen: ":9090"
  path: /metrics

serve:
  log_level: info
```

Go additions to `config.go`:

```go
type Config struct {
	Brokers        []string      `mapstructure:"brokers"`
	ClusterID      string        `mapstructure:"cluster_id"`
	ScrapeInterval string        `mapstructure:"scrape_interval"`
	Storage        StorageConfig `mapstructure:"storage"`
	Kafka          KafkaConfig   `mapstructure:"kafka"`
	Alerts         []AlertRule   `mapstructure:"alerts"`
	Prometheus     PromConfig    `mapstructure:"prometheus"`
	Serve          ServeConfig   `mapstructure:"serve"`
}

type KafkaConfig struct {
	TLS  TLSConfig  `mapstructure:"tls"`
	SASL SASLConfig `mapstructure:"sasl"`
}

type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

type SASLConfig struct {
	Mechanism   string `mapstructure:"mechanism"`
	Username    string `mapstructure:"username"`
	PasswordEnv string `mapstructure:"password_env"`
}
```

## Loading API

```go
// Load reads defaults, optional file, env, and applies flag overrides via viper.
func Load(flagSet *pflag.FlagSet) (*Config, error)

// ScrapeInterval returns the parsed interval with validation (>= 1s, <= 1m).
func (c *Config) ScrapeInterval() (time.Duration, error)
```

`Load` flow:

1. `viper.SetConfigFile(path)` for the first existing candidate (or none).
2. `viper.ReadInConfig()` (ignore not-found).
3. Bind known env keys with the `STREAMPULSE_` prefix.
4. `viper.Unmarshal(&cfg)` with `DecodeHook` for `time.Duration` from strings.
5. **Validate** — returns `ValidationError` listing all problems (unknown storage type, alert condition syntax, SASL mechanism, empty brokers, missing password env). See `alerts.md` for the condition grammar; validation is the first consumer.

## Wiring

- Root command: `PreRunE` loads config, stores it on the command context; `--brokers` flag overrides `cfg.Brokers`.
- `serve`/`check`/`alerts`/`analyze`/`dlq`: same `PreRunE`; each reads only its section.
- TUI: `runTUI` passes `cfg` (brokers + auth) into `kafka.NewClient` (see `security.md`).

## Secrets handling

- Never log config values. `SASLConfig.PasswordEnv` names an env var; the password is resolved at connect time only.
- Webhook URLs likewise via `*_env` fields (`alerts.md`).
- `fmt.Sprintf("%+v", cfg)` is forbidden in logs; add `func (c *Config) Redacted() map[string]any` for debug output.

## Failure modes

- Missing config file → defaults (not an error).
- Malformed YAML → clear error with file+line (viper does this).
- Validation failure → exit code 2 with all violations listed (matches `health-check.md` exit-code contract).
- Env var typo → validation catches `password_env` pointing at an unset variable when SASL is configured.

## Testing

- Table-driven precedence tests: defaults only, file overrides default, env overrides file, flag overrides env.
- YAML golden files under `internal/config/testdata/`.
- `Redacted()` never contains `password`/`webhook` values (assert on output).
- Validation: each violation type produces the expected message; unknown storage type uses the same error text as `storage.NewStore`.
