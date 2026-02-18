package source

import (
	"context"
	"log/slog"
	"strings"

	"github.com/gocolly/colly/v2"
	"github.com/shanehull/sourcerer/internal/model"
)

type NorthLinkScraper struct {
	logger    *slog.Logger
	startURL  string
	category  string
	source    string
}

func NewNorthLinkScraper(logger *slog.Logger, url, category, source string) *NorthLinkScraper {
	return &NorthLinkScraper{
		logger:    logger,
		startURL:  url,
		category:  category,
		source:    source,
	}
}

func (s *NorthLinkScraper) Name() string { return s.source }

func (s *NorthLinkScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	c := colly.NewCollector(
		colly.AllowedDomains("northlink.org.au"),
		// User Agent helps avoid basic bot detection on WordPress sites
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"),
	)

	// TARGET: Only company headings that have both a title AND a link
	// This filters out page headings like "Partnering with the Best..." which don't link to companies
	c.OnHTML(".elementor-widget-heading", func(e *colly.HTMLElement) {
		title := e.ChildAttr(".elementor-heading-title a", "href")
		name := strings.TrimSpace(e.ChildText(".elementor-heading-title a"))

		// Only process if we have BOTH a name AND a URL link
		// Page headings won't have links, only company entries do
		if name != "" && len(name) > 2 && title != "" {
			leads = append(leads, model.Lead{
				Name:        name,
				Category:    s.category,
				Sources:     []string{s.source},
				BusinessURL: title,
				FoundAtURL:  e.Request.URL.String(),
			})
		}
	})

	s.logger.Info("Starting NorthLink Scrape", "url", s.startURL)
	err := c.Visit(s.startURL)

	if len(leads) == 0 {
		s.logger.Warn("NorthLink scrape yielded 0 leads. The site might be lazy-loading or using a different Elementor widget.")
	}

	return leads, err
}
