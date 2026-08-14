package index

import (
	"context"

	"github.com/blugelabs/bluge"
)

// FacetResult holds aggregated counts for a single facet field.
type FacetResult struct {
	Field  string         `json:"field"`
	Values map[string]int `json:"values"` // value → count
}

// ComputeFacets scans search results and aggregates counts for the requested
// metadata fields. Uses the Bluge stored field values from matching documents.
func (b *BlugeStore) ComputeFacets(ctx context.Context, textQuery string, filters map[string]string, facetFields []string, maxResults int) ([]FacetResult, error) {
	if len(facetFields) == 0 {
		return nil, nil
	}

	reader, err := b.writer.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Build query (same as SearchWithScores)
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

	if maxResults <= 0 {
		maxResults = 10000
	}
	req := bluge.NewTopNSearch(maxResults, bq)
	dmi, err := reader.Search(ctx, req)
	if err != nil {
		return nil, err
	}

	// Initialize facet counters
	facets := make(map[string]map[string]int, len(facetFields))
	facetSet := make(map[string]bool, len(facetFields))
	for _, f := range facetFields {
		facets[f] = make(map[string]int)
		facetSet[f] = true
	}

	match, err := dmi.Next()
	for err == nil && match != nil {
		_ = match.VisitStoredFields(func(field string, value []byte) bool {
			if facetSet[field] {
				val := string(value)
				if val != "" {
					facets[field][val]++
				}
			}
			return true
		})
		match, err = dmi.Next()
	}

	// Build results
	results := make([]FacetResult, 0, len(facetFields))
	for _, f := range facetFields {
		results = append(results, FacetResult{
			Field:  f,
			Values: facets[f],
		})
	}
	return results, nil
}

// ComputeFacetsFromIDs computes facets from a set of pre-filtered document IDs.
// More efficient when used after hybrid search since we already know the result set.
func (b *BlugeStore) ComputeFacetsFromIDs(ctx context.Context, docIDs []uint32, facetFields []string) ([]FacetResult, error) {
	if len(facetFields) == 0 || len(docIDs) == 0 {
		return nil, nil
	}

	docs, err := b.GetDocumentsByIDs(ctx, docIDs)
	if err != nil {
		return nil, err
	}

	facets := make(map[string]map[string]int, len(facetFields))
	facetSet := make(map[string]bool, len(facetFields))
	for _, f := range facetFields {
		facets[f] = make(map[string]int)
		facetSet[f] = true
	}

	for _, doc := range docs {
		for field, value := range doc.Metadata {
			if facetSet[field] && value != "" {
				facets[field][value]++
			}
		}
	}

	results := make([]FacetResult, 0, len(facetFields))
	for _, f := range facetFields {
		results = append(results, FacetResult{
			Field:  f,
			Values: facets[f],
		})
	}
	return results, nil
}

