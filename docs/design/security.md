# Design: Kafka client security (SASL / TLS / mTLS / IAM)

**Status:** Design · **Depends on:** `configuration.md` · **Serves:** scraper, daemon, TUI, check, dlq

## Goal

Give `internal/kafka.Client` first-class auth support. AGENTS.md requires "SASL/SSL/mTLS/IAM support from day 1"; the current client only does plaintext (`kafka.Dialer{Timeout: 5s}`, `client.go:28`).

## Approach

kafka-go supports TLS and SASL natively through `kafka.Dialer` / `kafka.Transport`:

```go
dialer := &kafka.Dialer{
	Timeout:       5 * time.Second,
	TLS:           tlsConfig,          // nil => plaintext
	SASLMechanism: saslMechanism,      // nil => no auth
}
transport := &kafka.Transport{Dial: dialer.DialFunc, TLS: tlsConfig, SASL: saslMechanism}
```

## Constructor changes

`internal/kafka/client.go`:

```go
// Options configures broker connectivity.
type Options struct {
	TLS  tlsOptions
	SASL saslOptions
}

func NewClient(brokers []string) *Client            // unchanged, plaintext
func NewClientWithOptions(brokers []string, opts Options) (*Client, error)
```

`NewClientWithOptions`:

1. Build `*tls.Config`:
   - `CAFile` → `RootCAs` pool (must exist/parse, else error).
   - `CertFile`+`KeyFile` → `Certificates` (mTLS).
   - `InsecureSkipVerify` honored only with an explicit warning log.
2. Build `sasl.Mechanism` (import `github.com/segmentio/kafka-go/sasl` + `.../sasl/plain`, `.../sasl/scram`):
   - `plain` → `plain.Mechanism{Username, Password}`.
   - `scram-sha-256` / `scram-sha-512` → `scram.Mechanism(scram.SHA256, user, pass)`.
3. Reuse the existing hoisted `dialer`/`transport`/`adminClient` pattern so `Close()` still releases everything.
4. The `dial()` failover loop is unchanged — auth applies per-dial.

## IAM (AWS MSK IAM)

kafka-go has no built-in IAM mechanism, but the `sasl.Mechanism` interface is public:

```go
type Mechanism interface {
	Name() string
	Authenticate(ctx context.Context, saslState) error
}
```

Design: `internal/kafka/sasliam/` package implementing the AWS MSK IAM OAuth2-style SASL exchange (AWS_SIGV4), signed with `crypto/hmac` + the credentials provider interface:

```go
type CredentialsProvider interface {
	Credentials(ctx context.Context) (AccessKey, SecretKey, Token string, Err error)
}
```

- Default impl reads `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` env.
- Config: `kafka.sasl.mechanism: aws-iam` selects it.
- AWS SigV4 is implemented with stdlib only (`crypto/hmac`, `encoding/hex`, `net/http` canonical request helpers) — no SDK dependency.
- This is an isolated package; if it proves too large it ships in a follow-up without blocking plain/SCRAM/TLS.

## Connection rules

- TLS + SASL compose independently (any combination).
- With `TLS.Enabled=false`, plaintext (existing behavior).
- Every dial path gets the same dialer: `dial()`, `ListConsumerGroups` pre-flight dial, `groupsFromBroker` transport, and the future scraper reader/writer (see `scraper.md`/`dlq.md`).
- Broker list may use either `host:port` (plain) or `ssl://host:port` / `sasl_ssl://host:port` URL form; normalize in `NewClientWithOptions` (kafka-go's `ParseURL` conventions), documented in `configuration.md`.

## Failure modes

- Wrong password / missing cert → dial error wrapped with broker address; failover tries next broker (existing behavior).
- TLS hostname mismatch → error with the offending broker hostname (never the private key material).
- Password resolved from env at construction; a missing env var is a config validation error (see `configuration.md`).
- Never log credentials; errors from `tls.Config` and `sasl.Mechanism` must be sanitized (strip file contents — Go's own errors already are).

## Testing

- Unit: `NewClientWithOptions` table tests — TLS file loading (temp files), mechanism selection per `sasl.mechanism`, invalid combos (`scram` without password, unknown mechanism).
- `Options` → dialer/transport field assertions (TLS != nil, SASLMechanism type).
- Integration (skip-without-broker, existing pattern): plaintext still works against the docker broker; TLS/SCRAM against a locally started `apache/kafka` container with SASL config (docker-compose profile `secured`, added later) — gated by `STREAMPULSE_TEST_BROKER`.
- IAM: unit-test the SigV4 signing against AWS published test vectors (public values only, no real credentials in tests).
