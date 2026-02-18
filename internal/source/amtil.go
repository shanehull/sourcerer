package source

import (
	"context"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/shanehull/sourcerer/internal/model"
)

type AMTILScraper struct {
	logger   *slog.Logger
	startURL string
}

func NewAMTILScraper(logger *slog.Logger) *AMTILScraper {
	return &AMTILScraper{
		logger:   logger,
		startURL: "https://amtil.com.au/directory/",
	}
}

func (s *AMTILScraper) Name() string { return "AMTIL" }

func getRandomUserAgent() string {
	uas := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return uas[r.Intn(len(uas))]
}

func (s *AMTILScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead
	c := colly.NewCollector(colly.AllowedDomains("amtil.com.au", "www.amtil.com.au"))
	ua := getRandomUserAgent()

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", ua)
		if strings.Contains(r.URL.String(), "directory") {
			r.Headers.Set("Referer", "https://amtil.com.au/")
		}
	})

	c.Limit(&colly.LimitRule{DomainGlob: "*amtil.com.au*", Parallelism: 1, Delay: 2 * time.Second, RandomDelay: 3 * time.Second})

	c.OnHTML("tr", func(e *colly.HTMLElement) {
		name := strings.TrimSpace(e.ChildText("td:nth-child(1)"))
		location := strings.TrimSpace(e.ChildText("td:nth-child(2)"))

		if len(name) > 2 && s.isValidCompany(name) {
			state := ""
			parts := strings.Fields(location)
			if len(parts) > 0 {
				lastPart := strings.ToUpper(parts[len(parts)-1])
				switch lastPart {
				case "VIC", "NSW", "QLD", "WA", "SA", "TAS", "ACT", "NT":
					state = lastPart
				}
			}

			leads = append(leads, model.Lead{
				Name:       name,
				Category:   "Manufacturing",
				State:      state,
				Sources:    []string{s.Name()},
				FoundAtURL: e.Request.URL.String(),
			})
		}
	})

	s.logger.Info("Warming up session...", "ua", ua)
	_ = c.Visit("https://amtil.com.au/")
	time.Sleep(1 * time.Second)

	if err := c.Visit(s.startURL); err != nil {
		return nil, err
	}
	c.Wait()
	return leads, nil
}

func (s *AMTILScraper) isValidCompany(name string) bool {
	lower := strings.ToLower(name)
	return !(lower == "" || lower == "company name" || lower == "location" || strings.Contains(lower, "amtil"))
}
