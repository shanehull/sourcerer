package source

import (
	"context"
	"html"
	"log/slog"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/shanehull/sourcerer/internal/model"
)

type SEMMAScraper struct {
	logger   *slog.Logger
	startURL string
}

func NewSEMMAScraper(logger *slog.Logger) *SEMMAScraper {
	return &SEMMAScraper{
		logger:   logger,
		startURL: "https://semma.com.au/members-directory/",
	}
}

func (s *SEMMAScraper) Name() string { return "SEMMA" }

func (s *SEMMAScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	seen := make(map[string]bool)

	c := colly.NewCollector(colly.AllowedDomains("semma.com.au", "www.semma.com.au"))
	ua := getRandomUserAgent() // Re-using from amtil.go

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", ua)
	})

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*semma.com.au*",
		Parallelism: 1,
		Delay:       2 * time.Second,
		RandomDelay: 3 * time.Second,
	})

	c.OnHTML("h3.entry-title a", func(e *colly.HTMLElement) {
		name := strings.TrimSpace(html.UnescapeString(e.Text))

		if len(name) > 2 && !seen[name] {
			seen[name] = true
			leads = append(leads, model.Lead{
				Name:       name,
				Category:   "Manufacturing", // SEMMA is a manufacturing alliance
				State:      "VIC",           // South East Melbourne
				Sources:    []string{s.Name()},
				FoundAtURL: e.Request.URL.String(),
			})
		}
	})

	var scrapeErr error
	c.OnError(func(r *colly.Response, err error) {
		s.logger.Error("SEMMA Colly error", "url", r.Request.URL, "err", err)
		scrapeErr = err
	})

	s.logger.Info("Warming up SEMMA session...", "ua", ua)
	_ = c.Visit("https://semma.com.au/")

	if err := c.Visit(s.startURL); err != nil {
		return nil, err
	}
	c.Wait()

	if scrapeErr != nil {
		return nil, scrapeErr
	}

	return leads, nil
}
