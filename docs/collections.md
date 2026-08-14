# Collections

Collections are logical containers that group related data sources into a single searchable unit. Each collection owns its own `HybridEngine` (Bluge + HNSW-IF + Arena) for physical data isolation.

A `_default` collection is auto-created at startup. Documents without an explicit collection land here.

## Create a Collection

```bash
curl -s -X POST http://localhost:8000/v1/collections \
  -H "Content-Type: application/json" \
  -d '{"id": "engineering-docs", "name": "Engineering Docs"}' | jq
```

## Ingest into a Collection

Set `_collection_id` in document metadata when ingesting. Records without it go to `_default`.

## Search a Single Collection

```bash
curl -s -X POST http://localhost:8000/v1/collections/engineering-docs/search \
  -H "Content-Type: application/json" \
  -d '{"query": "deployment guide", "top_k": 5}' | jq
```

## Cross-Collection Search

Fan-out across multiple (or all) collections, merged via RRF:

```bash
# Search specific collections
curl -s -X POST http://localhost:8000/v1/collections/search \
  -H "Content-Type: application/json" \
  -d '{"query": "API reference", "collections": ["engineering-docs", "api-specs"], "top_k": 10}' | jq

# Search ALL collections (omit or pass empty collections array)
curl -s -X POST http://localhost:8000/v1/collections/search \
  -H "Content-Type: application/json" \
  -d '{"query": "API reference", "top_k": 10}' | jq
```

## Legacy Search (backward compatible)

`POST /search` always routes to the `_default` collection. No client changes needed.

## Three Search Tiers

| Endpoint | Behavior |
|----------|----------|
| `POST /search` | Routes to `_default` collection (backward compat) |
| `POST /v1/collections/{id}/search` | Search a specific collection |
| `POST /v1/collections/search` | Fan-out across selected/all collections, merge via RRF |

## Manage Collections

```bash
# List all collections
curl -s http://localhost:8000/v1/collections | jq

# Get collection details
curl -s http://localhost:8000/v1/collections/engineering-docs | jq

# Get collection stats (HNSW nodes, connector count)
curl -s http://localhost:8000/v1/collections/engineering-docs/stats | jq

# Bind a connector to a collection
curl -s -X POST http://localhost:8000/v1/collections/engineering-docs/connectors \
  -d '{"connector_job_id": "confluence-engineering"}'

# Delete a collection (cascading — removes all data)
curl -s -X DELETE http://localhost:8000/v1/collections/engineering-docs
```

Note: The `_default` collection cannot be deleted.
