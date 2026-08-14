# API Reference

Full OpenAPI 3.1 specification at [`api/openapi.yaml`](../api/openapi.yaml).

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search` | POST | Hybrid search (→ `_default` collection) |
| `/v1/collections` | POST | Create a collection |
| `/v1/collections` | GET | List collections |
| `/v1/collections/{id}` | GET | Get collection details |
| `/v1/collections/{id}` | PUT | Update collection |
| `/v1/collections/{id}` | DELETE | Delete collection (cascading) |
| `/v1/collections/{id}/search` | POST | Search a single collection |
| `/v1/collections/search` | POST | Cross-collection search (RRF merge) |
| `/v1/collections/{id}/connectors` | POST | Bind connector to collection |
| `/v1/collections/{id}/connectors/{cid}` | DELETE | Unbind connector |
| `/v1/collections/{id}/stats` | GET | Collection stats |
| `/v1/rag` | POST | RAG with citations |
| `/v1/suggest` | GET | Autocomplete suggestions |
| `/v1/documents` | DELETE | Delete by document IDs |
| `/v1/documents/query` | DELETE | Delete by metadata filter |
| `/health` | GET | Liveness probe |
| `/ready` | GET | Readiness probe (checks embedder reachability) |

## Go Client SDK

```go
import "github.com/efathom/yase/client"

c := client.New("http://localhost:8000", "your-api-key")

// Search
resp, _ := c.Search(ctx, client.SearchRequest{
    Query: "hybrid search engine",
    TopK:  10,
})

// RAG
rag, _ := c.RAG(ctx, client.RAGRequest{
    Query:       "How does HNSW work?",
    IncludeText: true,
})

// Delete
c.Delete(ctx, []uint32{123, 456})
```
