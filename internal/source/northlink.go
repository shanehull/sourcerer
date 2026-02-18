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

func (s *NorthLinkScraper) Name() string { return "NorthLink" }

func (s *NorthLinkScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	c := colly.NewCollector(
		colly.AllowedDomains("northlink.org.au"),
		// User Agent helps avoid basic bot detection on WordPress sites
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"),
	)

	// TARGET: The Elementor Heading Widget which contains the company name and link
	c.OnHTML(".elementor-widget-heading", func(e *colly.HTMLElement) {
		name := strings.TrimSpace(e.ChildText(".elementor-heading-title"))
		website := e.ChildAttr(".elementor-heading-title a", "href")

		// Validation to ensure we have a name and it's not a generic page heading
		if name != "" && len(name) > 1 && !strings.EqualFold(name, "Manufacturers") && !strings.EqualFold(name, "Service Providers") {
			leads = append(leads, model.Lead{
				Name:       name,
				Category:   s.category,
				Sources:    []string{s.source},
				FoundAtURL: website,
			})
		}
	})

	// FALLBACK: If names are in the text editor widget instead of headings
	c.OnHTML(".elementor-widget-text-editor", func(e *colly.HTMLElement) {
		// Only grab bold text if we haven't already filled our leads (safety)
		e.ForEach("p strong", func(_ int, el *colly.HTMLElement) {
			name := strings.TrimSpace(el.Text)
			if len(name) > 3 && !strings.Contains(name, "(") { // Skip phone numbers
				leads = append(leads, model.Lead{
					Name:     name,
					Category: s.category,
					Sources:  []string{s.source},
				})
			}
		})
	})

	s.logger.Info("Starting NorthLink Scrape", "url", s.startURL)
	err := c.Visit(s.startURL)

	if len(leads) == 0 {
		s.logger.Warn("NorthLink scrape yielded 0 leads. The site might be lazy-loading or using a different Elementor widget.")
	}

	return leads, err
}
