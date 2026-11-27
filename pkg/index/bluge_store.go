package index

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/blugelabs/bluge"
	blugeindex "github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
)

// SearchHit represents a single result from a Bluge text search.
type SearchHit struct {
	ID       string
	Score    float64
	Content  string
	Metadata map[string]string
}

// Reserved Bluge field names. User metadata may not use them: "_id" is the
// document identity (overwriting it would re-key the document), "content" is
// the analyzed body and "_all" is the composite field.
const (
	fieldID      = "_id"
	fieldContent = "content"
	fieldAll     = "_all"
)

func isReservedField(k string) bool {
	return k == fieldID || k == fieldContent || k == fieldAll || k == ""
}

// errStopIteration is used internally to end a match iteration early.
var errStopIteration = errors.New("stop iteration")

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

func docIDString(id uint32) string { return strconv.FormatUint(uint64(id), 10) }

func parseDocID(s string) (uint32, bool) {
	id, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}

// buildDocument converts content + metadata into a Bluge document, skipping
// reserved field names so metadata can never overwrite the document identity.
func buildDocument(docID string, content string, metadata map[string]string) *bluge.Document {
	doc := bluge.NewDocument(docID)
	doc.AddField(bluge.NewTextField(fieldContent, content).StoreValue().SearchTermPositions())
	for k, v := range metadata {
		if isReservedField(k) {
			continue
		}
		doc.AddField(bluge.NewKeywordField(k, v).StoreValue())
	}
	doc.AddField(bluge.NewCompositeFieldExcluding(fieldAll, nil))
	return doc
}

// IndexDocument adds or updates a single document in the Bluge index.
// Prefer ApplyBatch for bulk writes: every call is a separate durable batch.
func (b *BlugeStore) IndexDocument(docID string, content string, metadata map[string]string) error {
	doc := buildDocument(docID, content, metadata)
	return b.writer.Update(doc.ID(), doc)
}

// batchDoc is a document staged for ApplyBatch.
type batchDoc struct {
	id       uint32
	content  string
	metadata map[string]string
}

// ApplyBatch deletes and upserts documents in a single Bluge batch. Bluge
// applies a batch atomically and (with the default config) waits for it to be
// persisted, so a replace-by-URL either fully happens or not at all.
func (b *BlugeStore) ApplyBatch(deleteIDs []uint32, docs []batchDoc) error {
	if len(deleteIDs) == 0 && len(docs) == 0 {
		return nil
	}
	batch := blugeindex.NewBatch()
	for _, id := range deleteIDs {
		batch.Delete(bluge.Identifier(docIDString(id)))
	}
	for _, d := range docs {
		doc := buildDocument(docIDString(d.id), d.content, d.metadata)
		batch.Update(doc.ID(), doc)
	}
	return b.writer.Batch(batch)
}

// filterQuery builds a conjunction of exact-match metadata filters. Filter
// clauses carry zero boost so they constrain without affecting BM25 scores.
func filterQuery(filters map[string]string) (*bluge.BooleanQuery, bool) {
	bq := bluge.NewBooleanQuery()
	has := false
	for k, v := range filters {
		bq.AddMust(bluge.NewTermQuery(v).SetField(k).SetBoost(0))
		has = true
	}
	return bq, has
}

// textQuery builds "text MUST match AND filters". When text is empty it
// returns a filter-only (or match-all) query.
func textQuery(text string, filters map[string]string) bluge.Query {
	bq, has := filterQuery(filters)
	if text != "" {
		bq.AddMust(bluge.NewMatchQuery(text).SetField(fieldContent))
		has = true
	}
	if !has {
		return bluge.NewMatchAllQuery()
	}
	return bq
}

// Search executes a combined text + metadata filter query and returns the
// top N results with BM25 scores.
func (b *BlugeStore) Search(ctx context.Context, textQ string, filters map[string]string, topN int) ([]SearchHit, error) {
	return b.searchPage(ctx, textQ, filters, 0, topN)
}

// SearchAllPages invokes fn for every document in the index. It streams the
// whole index in a single pass (no deep pagination).
func (b *BlugeStore) SearchAllPages(ctx context.Context, _ int, fn func(SearchHit) error) error {
	return b.ForEach(ctx, nil, true, fn)
}

// ForEach streams every document matching filters (all documents when
// filters is empty) to fn. When withContent is false only the ID and
// metadata are loaded. Returning an error from fn stops the iteration and is
// propagated to the caller.
func (b *BlugeStore) ForEach(ctx context.Context, filters map[string]string, withContent bool, fn func(SearchHit) error) error {
	reader, err := b.writer.Reader()
	if err != nil {
		return fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	var q bluge.Query = bluge.NewMatchAllQuery()
	if bq, has := filterQuery(filters); has {
		q = bq
	}
	dmi, err := reader.Search(ctx, bluge.NewAllMatches(q))
	if err != nil {
		return fmt.Errorf("bluge search: %w", err)
	}
	match, err := dmi.Next()
	for err == nil && match != nil {
		hit, verr := visitHit(match, withContent)
		if verr != nil {
			return verr
		}
		if ferr := fn(hit); ferr != nil {
			return ferr
		}
		match, err = dmi.Next()
	}
	if err != nil {
		return fmt.Errorf("bluge iterate: %w", err)
	}
	return nil
}

func visitHit(match *search.DocumentMatch, withContent bool) (SearchHit, error) {
	hit := SearchHit{Score: match.Score, Metadata: make(map[string]string)}
	err := match.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case fieldID:
			hit.ID = string(value)
		case fieldContent:
			if withContent {
				hit.Content = string(value)
			}
		default:
			hit.Metadata[field] = string(value)
		}
		return true
	})
	if err != nil {
		return hit, fmt.Errorf("bluge visit fields: %w", err)
	}
	return hit, nil
}

// visitID extracts only the numeric document ID from a match.
func visitID(match *search.DocumentMatch) (uint32, bool) {
	var idStr string
	_ = match.VisitStoredFields(func(field string, value []byte) bool {
		if field == fieldID {
			idStr = string(value)
			return false
		}
		return true
	})
	return parseDocID(idStr)
}

// searchPage executes the combined query with pagination (from/size).
func (b *BlugeStore) searchPage(ctx context.Context, textQ string, filters map[string]string, from, size int) ([]SearchHit, error) {
	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	req := bluge.NewTopNSearch(size, textQuery(textQ, filters)).SetFrom(from)
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	var hits []SearchHit
	match, err := dmi.Next()
	for err == nil && match != nil {
		hit, verr := visitHit(match, true)
		if verr != nil {
			return nil, verr
		}
		hits = append(hits, hit)
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}
	return hits, nil
}

// SearchWithScores returns the top maxResults documents that match textQ
// (and satisfy filters), mapped to their BM25 score. An empty text query has
// no lexical signal and returns an empty map.
func (b *BlugeStore) SearchWithScores(ctx context.Context, textQ string, filters map[string]string, maxResults int) (map[uint32]float64, error) {
	scores := make(map[uint32]float64)
	if textQ == "" || maxResults <= 0 {
		return scores, nil
	}
	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	req := bluge.NewTopNSearch(maxResults, textQuery(textQ, filters))
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	match, err := dmi.Next()
	for err == nil && match != nil {
		if id, ok := visitID(match); ok && match.Score > 0 {
			scores[id] = match.Score
		}
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}
	return scores, nil
}

// FilteredIDs returns the IDs of documents matching filters, stopping as soon
// as more than limit matches are seen. overflow reports that the filter
// matches more than limit documents (ids is then incomplete). A limit < 0
// means unlimited.
func (b *BlugeStore) FilteredIDs(ctx context.Context, filters map[string]string, limit int) (ids []uint32, overflow bool, err error) {
	err = b.ForEach(ctx, filters, false, func(h SearchHit) error {
		if limit >= 0 && len(ids) >= limit {
			overflow = true
			return errStopIteration
		}
		if id, ok := parseDocID(h.ID); ok {
			ids = append(ids, id)
		}
		return nil
	})
	if errors.Is(err, errStopIteration) {
		err = nil
	}
	return ids, overflow, err
}

// SearchFiltered returns the set of document IDs matching the metadata
// filters (up to maxResults; < 0 for unlimited).
func (b *BlugeStore) SearchFiltered(ctx context.Context, filters map[string]string, maxResults int) (map[uint32]bool, error) {
	ids, _, err := b.FilteredIDs(ctx, filters, maxResults)
	if err != nil {
		return nil, err
	}
	allowed := make(map[uint32]bool, len(ids))
	for _, id := range ids {
		allowed[id] = true
	}
	return allowed, nil
}

// MatchIDs reports which of ids exist and satisfy filters. Its cost is
// proportional to len(ids), not to the number of documents the filter
// matches, which makes it the right post-filter for ANN candidates.
func (b *BlugeStore) MatchIDs(ctx context.Context, ids []uint32, filters map[string]string) (map[uint32]bool, error) {
	out := make(map[uint32]bool, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	reader, err := b.writer.Reader()
	if err != nil {
		return nil, fmt.Errorf("bluge reader: %w", err)
	}
	defer reader.Close()

	bq, _ := filterQuery(filters)
	idq := bluge.NewBooleanQuery()
	for _, id := range ids {
		idq.AddShould(bluge.NewTermQuery(docIDString(id)).SetField(fieldID))
	}
	idq.SetMinShould(1)
	bq.AddMust(idq)

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(bq))
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}
	match, err := dmi.Next()
	for err == nil && match != nil {
		if id, ok := visitID(match); ok {
			out[id] = true
		}
		match, err = dmi.Next()
	}
	if err != nil {
		return nil, fmt.Errorf("bluge iterate: %w", err)
	}
	return out, nil
}

// GetDocumentsByIDs retrieves full stored content and metadata for a set of
// document IDs in a single batched query.
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
		bq.AddShould(bluge.NewTermQuery(docIDString(id)).SetField(fieldID))
	}
	bq.SetMinShould(1)

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(bq))
	if err != nil {
		return nil, fmt.Errorf("bluge search: %w", err)
	}

	match, err := dmi.Next()
	for err == nil && match != nil {
		hit, verr := visitHit(match, true)
		if verr != nil {
			return nil, verr
		}
		if id, ok := parseDocID(hit.ID); ok {
			results[id] = hit
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
