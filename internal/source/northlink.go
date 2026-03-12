package source

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	app "github.com/lib4u/fake-useragent"
	"github.com/shanehull/sourcerer/internal/model"
)

type NorthLinkScraper struct {
	logger    *slog.Logger
	startURL  string
	category  string
	source    string
	ua        *app.UserAgent
}

func NewNorthLinkScraper(logger *slog.Logger, url, category, source string) *NorthLinkScraper {
	return &NorthLinkScraper{
		logger:    logger,
		startURL:  url,
		category:  category,
		source:    source,
		ua:        NewUserAgent(logger),
	}
}

func (s *NorthLinkScraper) Name() string { return s.source }

func (s *NorthLinkScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	
	c := colly.NewCollector(
		colly.AllowedDomains("northlink.org.au"),
		colly.UserAgent(GetRandomUserAgent(s.ua)),
	)
	c.SetClient(&http.Client{Timeout: 60 * time.Second})

	// TARGET: Company headings with links (h5 with elementor-heading-title class containing an <a> tag)
	// This filters out page headings without links
	c.OnHTML(".elementor-heading-title a", func(e *colly.HTMLElement) {
		name := strings.TrimSpace(e.Text)
		url := e.Attr("href")

		// Only process if we have BOTH a name AND a URL link
		// Require minimum name length to avoid spurious entries
		if name != "" && len(name) > 2 && url != "" {
			leads = append(leads, model.Lead{
				Name:        name,
				Category:    s.category,
				Sources:     []string{s.source},
				BusinessURL: url,
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
