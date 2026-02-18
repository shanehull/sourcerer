package source

import (
	"context"
	"log/slog"
	"strings"

	"github.com/shanehull/sourcerer/internal/enrich"
	"github.com/shanehull/sourcerer/internal/model"
)

type ABRSearchSource struct {
	logger   *slog.Logger
	client   *enrich.ABRClient
	keywords []string
}

func NewABRSearchSource(logger *slog.Logger, client *enrich.ABRClient, keywords []string) *ABRSearchSource {
	var clean []string
	for _, k := range keywords {
		trimmed := strings.TrimSpace(k)
		if trimmed != "" {
			clean = append(clean, trimmed)
		}
	}
	if len(clean) == 0 {
		clean = []string{"Precision Engineering", "CNC Machining", "Steel Fabrication"}
	}
	return &ABRSearchSource{
		logger:   logger,
		client:   client,
		keywords: clean,
	}
}

func (s *ABRSearchSource) Name() string { return "ABR-Search" }

func (s *ABRSearchSource) Fetch(ctx context.Context) ([]model.Lead, error) {
	var allLeads []model.Lead
	for _, kw := range s.keywords {
		s.logger.Info("Searching ABR", "keyword", kw)
		leads, err := s.client.SearchByName(ctx, kw)
		if err != nil {
			s.logger.Error("ABR search failed", "kw", kw, "err", err)
			continue
		}
		allLeads = append(allLeads, leads...)
	}
	return allLeads, nil
}
