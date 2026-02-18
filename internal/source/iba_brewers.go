package source

import (
	"context"
	"log/slog"
	"strings"

	"github.com/gocolly/colly/v2"
	"github.com/shanehull/sourcerer/internal/model"
)

type IBAScraper struct {
	logger   *slog.Logger
	startURL string
}

func NewIBAScraper(logger *slog.Logger) *IBAScraper {
	return &IBAScraper{
		logger:   logger,
		startURL: "https://independentbrewers.org.au/brewery-members/",
	}
}

func (s *IBAScraper) Name() string { return "IBA" }

func (s *IBAScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	c := colly.NewCollector(colly.AllowedDomains("independentbrewers.org.au"))

	// Target the specific x-col grid item from your HTML
	c.OnHTML(".x-col", func(e *colly.HTMLElement) {
		// Extract Name from the H3 primary text
		name := e.ChildText(".x-text-content-text-primary")

		// Extract Location/State from the content block
		// Note: HTML shows things like "ALEXANDRIA NSW" or "BALNARRING VIC"
		locationRaw := e.ChildText(".x-text.x-content")

		// Extract the specific brewery member link
		link := e.ChildAttr("a.x-image", "href")

		if name != "" {
			leads = append(leads, model.Lead{
				Name:       strings.TrimSpace(name),
				Category:   "Brewing/Manufacturing",
				Sources:    []string{s.Name()},
				FoundAtURL: link,
				// We pass the raw location string; the Enricher/ABR will resolve the formal State
				State: strings.TrimSpace(locationRaw),
			})
		}
	})

	s.logger.Info("Starting IBA Scrape", "url", s.startURL)
	err := c.Visit(s.startURL)

	if len(leads) == 0 {
		s.logger.Warn("IBA scrape yielded 0 results. Check if selectors have changed.")
	}

	return leads, err
}
