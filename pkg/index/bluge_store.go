package index

import (
	"context"
	"fmt"
	"strconv"

	"github.com/blugelabs/bluge"
)

// SearchHit represents a single result from a Bluge text search.
type SearchHit struct {
	ID       string
	Score    float64
	Content  string
	Metadata map[string]string
}

// BlugeStore wraps a Bluge inverted index for BM25 text search and
// Roaring Bitmap metadata filtering.
type BlugeStore struct {
	writer *bluge.Writer
}

// NewBlugeStore opens (or creates) a Bluge index at the given path.
func NewBlugeStore(indexPath string) (*BlugeStore, error) {
	config := bluge.DefaultConfig(indexPath)
	writer, err := bluge.OpenWriter(config)
	if err != nil {
		return nil, fmt.Errorf("bluge open writer: %w", err)
	}
	return &BlugeStore{writer: writer}, nil
}

// IndexDocument adds or updates a document in the Bluge index.
// content is analyzed for BM25 scoring; metadata entries become exact-match
// keyword fields backed by Roaring Bitmaps.
func (b *BlugeStore) IndexDocument(docID string, content string, metadata map[string]string) error {
	doc := bluge.NewDocument(docID)
	doc.AddField(bluge.NewTextField("content", content).StoreValue().SearchTermPositions())
	for k, v := range metadata {
		doc.AddField(bluge.NewKeywordField(k, v).StoreValue())
	}
	doc.AddField(bluge.NewCompositeFieldExcluding("_all", nil))
	return b.writer.Update(doc.ID(), doc)
}

// Search executes a combined text + metadata filter query and returns the
// top N results with BM25 scores.
func (b *BlugeStore) Search(ctx context.Context, textQuery string, filters map[string]string, topN int) ([]SearchHit, error) {
	return b.searchPage(ctx, textQuery, filters, 0, topN)
}

// SearchAllPages iterates over every document (no text query) in batches,
// invoking fn for each hit. Used by the TTL sweeper so it is not limited to
// the first N documents.
func (b *BlugeStore) SearchAllPages(ctx context.Context, batchSize int, fn func(SearchHit) error) error {
	from := 0
	for {
		hits, err := b.searchPage(ctx, "", nil, from, batchSize)
		if err != nil {
			return err
		}
		for _, h := range hits {
			if err := fn(h); err != nil {
				return err
			}
		}
		if len(hits) < batchSize {
			return nil
		}
		from += len(hits)
	}
}

// searchPage executes the combined query with pagination (from/size).
func (b *BlugeStore) searchPage(ctx context.Context, textQuery string, filters map[string]string, from, size int) ([]SearchHit, error) {
	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	bq := bluge.NewBooleanQuery()
	hasClause := false

	// MUST clauses: exact-match metadata filters
	for k, v := range filters {
		bq.AddMust(bluge.NewTermQuery(v).SetField(k))
		hasClause = true
	}

	// SHOULD clause: BM25 text query
	if textQuery != "" {
		bq.AddShould(bluge.NewMatchQuery(textQuery).SetField("content"))
		hasClause = true
	}

	// If no clauses, match all
	if !hasClause {
		bq.AddMust(bluge.NewMatchAllQuery())
	}

	req := bluge.NewTopNSearch(size, bq).SetFrom(from).WithStandardAggregations()
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	var hits []SearchHit
	match, err := dmi.Next()
	for err == nil && match != nil {
		hit := SearchHit{
			Score:    match.Score,
			Metadata: make(map[string]string),
		}
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case "_id":
				hit.ID = string(value)
			case "content":
				hit.Content = string(value)
			default:
				hit.Metadata[field] = string(value)
			}
			return true
		})
		if err != nil {
			return nil, fmt.Errorf("bluge visit fields: %w", err)
		}
		hits = append(hits, hit)
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}

	return hits, nil
}

// SearchWithScores executes a combined text + metadata filter query and returns
// a map of docID (as uint32) → BM25 score. Used by the hybrid searcher for
// pre-filtering before vector search.
func (b *BlugeStore) SearchWithScores(ctx context.Context, textQuery string, filters map[string]string, maxResults int) (map[uint32]float64, error) {
	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	bq := bluge.NewBooleanQuery()
	hasClause := false

	for k, v := range filters {
		bq.AddMust(bluge.NewTermQuery(v).SetField(k))
		hasClause = true
	}
	if textQuery != "" {
		bq.AddShould(bluge.NewMatchQuery(textQuery).SetField("content"))
		hasClause = true
	}
	if !hasClause {
		bq.AddMust(bluge.NewMatchAllQuery())
	}

	req := bluge.NewTopNSearch(maxResults, bq).WithStandardAggregations()
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	scores := make(map[uint32]float64)
	match, err := dmi.Next()
	for err == nil && match != nil {
		var idStr string
		_ = match.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				idStr = string(value)
			}
			return true
		})
		if idStr != "" {
			if id, parseErr := strconv.ParseUint(idStr, 10, 32); parseErr == nil {
				scores[uint32(id)] = match.Score
			}
		}
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}

	return scores, nil
}

// SearchFiltered returns the set of document IDs matching the metadata
// filters, without BM25 text scoring. This is the pre-filter "allowed set" —
// a filter-only query so text ranking cannot silently drop semantically
// relevant documents that pass the filters.
func (b *BlugeStore) SearchFiltered(ctx context.Context, filters map[string]string, maxResults int) (map[uint32]bool, error) {
	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	bq := bluge.NewBooleanQuery()
	hasClause := false
	for k, v := range filters {
		bq.AddMust(bluge.NewTermQuery(v).SetField(k))
		hasClause = true
	}
	if !hasClause {
		bq.AddMust(bluge.NewMatchAllQuery())
	}

	req := bluge.NewTopNSearch(maxResults, bq)
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	allowed := make(map[uint32]bool)
	match, err := dmi.Next()
	for err == nil && match != nil {
		var idStr string
		_ = match.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				idStr = string(value)
			}
			return true
		})
		if idStr != "" {
			if id, parseErr := strconv.ParseUint(idStr, 10, 32); parseErr == nil {
				allowed[uint32(id)] = true
			}
		}
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}
	return allowed, nil
}

// GetDocumentsByIDs retrieves full stored content and metadata for a set of
// document IDs. Used by the RAG API to populate citation text. A single
// batched query is used instead of one query per ID.
func (b *BlugeStore) GetDocumentsByIDs(ctx context.Context, ids []uint32) (map[uint32]SearchHit, error) {
	results := make(map[uint32]SearchHit, len(ids))
	if len(ids) == 0 {
		return results, nil
	}

	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	bq := bluge.NewBooleanQuery()
	for _, id := range ids {
		bq.AddShould(bluge.NewTermQuery(strconv.FormatUint(uint64(id), 10)).SetField("_id"))
	}
	bq.SetMinShould(1)

	req := bluge.NewTopNSearch(len(ids), bq)
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	match, err := dmi.Next()
	for err == nil && match != nil {
		hit := SearchHit{Metadata: make(map[string]string)}
		if verr := match.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case "_id":
				hit.ID = string(value)
			case "content":
				hit.Content = string(value)
			default:
				hit.Metadata[field] = string(value)
			}
			return true
		}); verr != nil {
			return nil, fmt.Errorf("bluge visit fields: %w", verr)
		}

		if hit.ID != "" {
			if id, perr := strconv.ParseUint(hit.ID, 10, 32); perr == nil {
				results[uint32(id)] = hit
			}
		}
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}

	return results, nil
}

// DeleteDocument removes a document from the Bluge index by ID.
func (b *BlugeStore) DeleteDocument(docID string) error {
	return b.writer.Delete(bluge.Identifier(docID))
}

// Close shuts down the Bluge writer.
func (b *BlugeStore) Close() error {
	return b.writer.Close()
}
