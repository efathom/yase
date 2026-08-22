package gateway

import (
	"net/http"
	"strconv"

	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/index"
)

// SuggestProvider provides autocomplete suggestions.
//
// Suggestions are drawn from the indexed term dictionary, which spans every
// tenant, so the filters argument is required to keep one tenant's terms —
// customer names, internal identifiers — out of another tenant's completions.
type SuggestProvider interface {
	Suggest(prefix string, limit int, filters map[string]string) []index.Suggestion
}

func (h *Handler) handleSuggest(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "q parameter required"})
		return
	}

	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 50 {
		limit = 50
	}

	// Check if searcher implements SuggestProvider
	sp, ok := h.Searcher.(SuggestProvider)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "suggest not supported"})
		return
	}

	filters := auth.InjectTenantFilter(auth.FromContext(r.Context()), nil)

	suggestions := sp.Suggest(q, limit, filters)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"query":       q,
		"suggestions": suggestions,
	})
}
