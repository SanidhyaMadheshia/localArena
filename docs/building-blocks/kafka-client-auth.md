# Kafka client auth (TLS + SCRAM)

Every Kafka client reads the same environment variables. With none set, clients
connect in plaintext exactly as before (Docker Compose is unchanged).

## Env var contract

| Variable | Values | Default | Notes |
|---|---|---|---|
| `KAFKA_BROKERS` | `host:port,...` | per service | Bootstrap list (existing variable). |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`, `SSL`, `SASL_SSL` | `PLAINTEXT` | Anything else is a startup error. |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512`, `SCRAM-SHA-256` | `SCRAM-SHA-512` | Only read for `SASL_SSL`. |
| `KAFKA_USERNAME` | KafkaUser name | none | Required for `SASL_SSL`. |
| `KAFKA_PASSWORD` | password | none | Required for `SASL_SSL`. Never logged or put in errors. |
| `KAFKA_CA_FILE` | PEM path | empty = system roots | An unreadable file or one with no certificate is an error. |

## How each language consumes it

- **Go** (`backend/internal/kafkaconf`): `FromEnv()` or `Load(getenv)` returns `[]kgo.Opt`
  (`kgo.DialTLSConfig` plus `kgo.SASL(scram...)`). Used by `events.New` (gameserver
  publisher), `rewards.Worker.consume`, and `cmd/eventcat`. Each prepends the options
  to its own, so a client with no env vars gets an empty list.
- **Java** (`streams/.../Main.java`): `Main.config()` maps the variables to
  `security.protocol`, `sasl.mechanism`, `sasl.jaas.config` (`ScramLoginModule`, values
  quoted and escaped), `ssl.truststore.type=PEM` and `ssl.truststore.location`. Nothing
  is set for `PLAINTEXT`/unset. PEM truststores need Kafka 3.3+.
- **Helm**: with `kafka.bootstrap` set, the gameserver Deployment gets `KAFKA_BROKERS`,
  and with `kafka.auth.enabled=true` also the rest, `KAFKA_PASSWORD` from the Secret
  named after the KafkaUser (key `password`), and the cluster CA Secret
  (`kafka.auth.caSecret`, key `ca.crt`) mounted at `/etc/kafka-ca`. `EVENT_SPOOL_DIR` points
  to `/var/lib/gameserver/spool` on an emptyDir (or a PVC with `gameserver.spool.pvc.enabled`),
  which works with `readOnlyRootFilesystem`.
  `values-eks.yaml` enables this against `localarena-kafka-bootstrap.data.svc:9093`.
  The api chart template has no Kafka client, so it is not wired. The chart has no
  rewards or streams Deployment yet; `kafka.users` only lists `gameserver`.

## Test it

```bash
cd backend && go test ./internal/kafkaconf/ ./internal/events/ ./internal/rewards/
cd streams && ./gradlew test --no-daemon          # MainConfigTest
helm template la infra/helm/localarena -f infra/helm/localarena/values-eks.yaml | kubectl apply --dry-run=client -f -
```

Against a real cluster, run `eventcat` with the variables set, from a pod that mounts
the user Secret and CA Secret:

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL KAFKA_USERNAME=localarena-eventcat KAFKA_PASSWORD=... \
KAFKA_CA_FILE=/etc/kafka-ca/ca.crt eventcat -brokers localarena-kafka-bootstrap.data.svc:9093 -from-start -n 1
```

Unit tests need no network. The Go tests only prove the options are built and accepted
by franz-go. They do not prove a broker handshake.

## Rotating the CA

The CA file is read once, when the client is created.

- **Go**: a running client keeps the old pool. After the CA changes, restart the pod
  (a mounted Secret updates on disk, but nothing re-reads it). `rewards` recreates its
  client whenever its consumer loop restarts, so it picks up a new file then.
- **Java**: Kafka loads the PEM truststore at client creation. Whether it reloads on
  file change is unknown here.

Not yet determined: how each client behaves when Strimzi renews the cluster CA
(overlap period, whether connections fail before a restart). Drill it in experiment 012.
Safe procedure until then: let Strimzi publish the new CA alongside the old one, then
rolling-restart every client.
