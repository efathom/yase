# Connectors & Embeddings

## Enterprise Connectors

YASE ships with 10 connectors that pull data from SaaS apps, databases, and file stores into the indexing pipeline. Each supports incremental sync via a persisted cursor.

| Connector | Auth | Incremental | Source |
|-----------|------|-------------|--------|
| Confluence | Basic/OAuth2/Bearer | `lastModified` cursor | SaaS |
| Jira | Basic/OAuth2/Bearer | JQL `updated` | SaaS |
| S3 | AWS credentials | `LastModified` | Cloud storage |
| PostgreSQL | Basic/mTLS | Timestamp column | Database |
| MySQL | Basic/mTLS | Timestamp column | Database |
| Google Drive | OAuth2 service account | `changes.list` delta | SaaS |
| Salesforce | OAuth2/Bearer | `SystemModstamp` | SaaS |
| SharePoint | OAuth2/Bearer | MS Graph delta queries | SaaS |
| Slack | Bearer/OAuth2 | `oldest` timestamp | SaaS |
| Twitter/X | Bearer/OAuth2 | `since_id` | SaaS |
| *Custom* | *Any* | *Plugin-defined* | go-plugin RPC |

### Running connectors

```bash
# Start the connector manager with a config file
go run cmd/connector/main.go --connectors configs/connectors.json

# Trigger a sync manually
curl -X POST http://localhost:9300/jobs/confluence-engineering/trigger
```

Connector output is bridged into the standard Kafka → index pipeline (`ConnectorBridge`), so ingested records flow through the same chunk → embed → index stages as crawler and direct-ingest data. Set `_collection_id` (or bind a connector to a collection) to route output into a specific collection.

Custom connectors can be written as standalone binaries using HashiCorp go-plugin; name them `yase-connector-<type>` and the manager discovers them automatically.

## Embedding Providers

| Provider | Model | MTEB | Latency | Config |
|----------|-------|------|---------|--------|
| **TEI** (default) | gte-modernbert-base | 64.38 | ~17ms (Metal) | `provider: tei` |
| Ollama | nomic-embed-text | 62.28 | ~117ms | `provider: ollama` |
| OpenAI | text-embedding-3-small | 62.26 | 100-500ms | `provider: openai` |
| Mock | deterministic hash | N/A | <1ms | `provider: mock` |

Embeddings are cached in an LRU to avoid redundant API calls for repeated text, and the default TEI provider runs fully self-hosted so no data leaves your infrastructure.
