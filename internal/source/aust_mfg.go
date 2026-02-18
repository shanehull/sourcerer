package source

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/shanehull/sourcerer/internal/model"
)

type AustMfgScraper struct {
	logger  *slog.Logger
	apiURL  string
	blockID string // The tdi_74 identifier from the HTML
}

func NewAustMfgScraper(logger *slog.Logger) *AustMfgScraper {
	return &AustMfgScraper{
		logger:  logger,
		apiURL:  "https://www.australianmanufacturing.com.au/wp-admin/admin-ajax.php",
		blockID: "tdi_74",
	}
}

func (s *AustMfgScraper) Name() string {
	return "AustMfg"
}

func (s *AustMfgScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var allLeads []model.Lead

	// We iterate through pages. If a page returns 0 leads, we assume we've hit the end.
	currentPage := 1

	for {
		s.logger.Info("Scraping AustMfg AJAX page", "page", currentPage)

		leads, err := s.fetchPage(ctx, currentPage)
		if err != nil {
			s.logger.Error("Error fetching AJAX page", "page", currentPage, "err", err)
			break
		}

		if len(leads) == 0 {
			s.logger.Info("No more leads found, finishing AustMfg scrape", "total", len(allLeads))
			break
		}

		allLeads = append(allLeads, leads...)

		// To be polite and avoid 429 Rate Limits
		time.Sleep(500 * time.Millisecond)
		currentPage++

		// Safety break to prevent infinite loops during testing
		if currentPage > 100 {
			break
		}
	}

	return allLeads, nil
}

func (s *AustMfgScraper) fetchPage(ctx context.Context, pageNum int) ([]model.Lead, error) {
	var leads []model.Lead

	c := colly.NewCollector(
		colly.AllowedDomains("www.australianmanufacturing.com.au"),
	)

	// Selector for the TagDiv module cards you identified
	c.OnHTML(".td_module_wrap", func(e *colly.HTMLElement) {
		name := e.ChildText(".entry-title a")
		category := e.ChildText(".td-post-category")
		link := e.ChildAttr(".entry-title a", "href")

		// Filter for directory entries only (ignoring news by checking slug)
		if name != "" && strings.Contains(link, "/business-directory/") {
			leads = append(leads, model.Lead{
				Name:       strings.TrimSpace(name),
				Category:   strings.TrimSpace(category),
				Sources:    []string{s.Name()},
				FoundAtURL: link,
			})
		}
	})

	// Construct the AJAX POST request parameters
	// These are extracted from the site's TagDiv Newspaper theme logic
	formData := url.Values{}
	formData.Set("action", "td_ajax_block")
	formData.Set("td_atts[block_id]", s.blockID)
	formData.Set("td_atts[td_column_number]", "3")
	formData.Set("td_atts[ajax_pagination]", "load_more")
	formData.Set("td_column_number", "3")
	formData.Set("td_current_page", fmt.Sprintf("%d", pageNum))
	formData.Set("block_id", s.blockID)

	// The endpoint specifically needs these query params as well
	targetURL := fmt.Sprintf("%s?td_theme_name=Newspaper&v=12.6.9", s.apiURL)

	err := c.Post(targetURL, map[string]string{
		"action":            "td_ajax_block",
		"td_atts[block_id]": s.blockID,
		"td_current_page":   fmt.Sprintf("%d", pageNum),
		"td_column_number":  "3",
		"td_block_id":       s.blockID,
	})
	if err != nil {
		return nil, err
	}

	return leads, nil
}
