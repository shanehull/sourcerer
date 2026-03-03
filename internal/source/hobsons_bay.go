package source

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	app "github.com/lib4u/fake-useragent"
	"github.com/shanehull/sourcerer/internal/model"
)

type HobsonsBayScraper struct {
	logger *slog.Logger
	ua     *app.UserAgent
}

func NewHobsonsBayScraper(logger *slog.Logger) *HobsonsBayScraper {
	return &HobsonsBayScraper{
		logger: logger,
		ua:     NewUserAgent(logger),
	}
}

func (s *HobsonsBayScraper) Name() string { return "HobsonsBay" }

func (s *HobsonsBayScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	maxPages := 27 // From the pagination UI
	
	c := colly.NewCollector(
		colly.UserAgent(GetRandomUserAgent(s.ua)),
	)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*hobsonsbaybusiness.com.au*",
		Parallelism: 1,
		Delay:       1 * time.Second,
		RandomDelay: 1 * time.Second,
	})

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		r.Headers.Set("Accept-Language", "en-US,en;q=0.5")
		r.Headers.Set("Referer", "https://www.hobsonsbaybusiness.com.au/")
	})

	// Hobsons Bay Business Directory uses OpenCities CMS with list-item-container structure
	c.OnHTML("div.list-item-container", func(e *colly.HTMLElement) {
		name := strings.TrimSpace(e.ChildText("h2.list-item-title"))

		if name != "" && len(name) > 2 {
			leads = append(leads, model.Lead{
				Name:       name,
				Category:   "General",
				Sources:    []string{s.Name()},
				FoundAtURL: e.Request.URL.String(),
			})
		}
	})

	s.logger.Info("Querying Hobsons Bay Business Directory", "url", "https://www.hobsonsbaybusiness.com.au/Business-Directory-Menu")
	err := c.Visit("https://www.hobsonsbaybusiness.com.au/Business-Directory-Menu")
	if err != nil {
		return nil, err
	}

	// Paginate through all pages using query parameter format
	for page := 2; page <= maxPages; page++ {
		pageURL := fmt.Sprintf("https://www.hobsonsbaybusiness.com.au/Business-Directory-Menu?dlv_OC%%20CL%%20Invest%%20Business%%20Directory%%20Listing=(pageindex=%d)", page)
		s.logger.Info("Fetching page", "page", page, "current_leads", len(leads))
		if err := c.Visit(pageURL); err != nil {
			s.logger.Error("Error fetching page", "page", page, "err", err)
			break
		}
	}

	c.Wait()

	if len(leads) == 0 {
		s.logger.Warn("Hobsons Bay scrape yielded 0 leads. The site structure may have changed.")
	}

	return leads, nil
}
