package source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shanehull/sourcerer/internal/model"
)

type RTOScraper struct {
	logger *slog.Logger
	apiURL string
}

func NewRTOScraper(logger *slog.Logger) *RTOScraper {
	return &RTOScraper{
		logger: logger,
		apiURL: "https://training.gov.au/api/search/organisation",
	}
}

func (s *RTOScraper) Name() string {
	return "RTO"
}

type tgaDataResponse struct {
	Count      int `json:"count"`
	TotalCount int `json:"totalCount"`
	Data       []struct {
		Code         string   `json:"code"`
		LegalName    string   `json:"legalName"`
		ABNs         []string `json:"abns"`
		Registration struct {
			StatusLabel string `json:"statusLabel"`
		} `json:"registration"`
		HeadOfficeAddress struct {
			State struct {
				Abbreviation string `json:"abbreviation"`
			} `json:"state"`
			PostCode string `json:"postCode"`
		} `json:"headOfficeAddress"`
	} `json:"data"`
}

func (s *RTOScraper) Fetch(ctx context.Context) ([]model.Lead, error) {
	var leads []model.Lead

	params := url.Values{}
	params.Add("api-version", "1.0")
	params.Add("searchText", "")
	params.Add("offset", "0")
	params.Add("pageSize", "50")
	params.Add("includeTotalCount", "true")
	params.Add("orderBy", "score desc")
	params.Add("filter", "(IsRto eq true)")

	fullURL := fmt.Sprintf("%s?%s", s.apiURL, params.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://training.gov.au/search")

	client := &http.Client{Timeout: 15 * time.Second}
	s.logger.Info("Querying RTO API", "url", s.apiURL)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("TGA API returned status %d", resp.StatusCode)
	}

	var tgaData tgaDataResponse
	if err := json.NewDecoder(resp.Body).Decode(&tgaData); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	for _, item := range tgaData.Data {
		if strings.EqualFold(item.Registration.StatusLabel, "Current") {
			abn := ""
			if len(item.ABNs) > 0 {
				// Clean spaces from ABN so it's a valid 11-digit Primary Key
				abn = strings.ReplaceAll(item.ABNs[0], " ", "")
			}

			leads = append(leads, model.Lead{
				ABN:        abn, // Primary Key
				Name:       item.LegalName,
				Category:   "Education/Training",
				State:      item.HeadOfficeAddress.State.Abbreviation,
				Postcode:   item.HeadOfficeAddress.PostCode,
				Sources:    []string{s.Name()},
				FoundAtURL: fmt.Sprintf("https://training.gov.au/organisation/details/%s", item.Code),
			})
		}
	}

	s.logger.Info("RTO API fetch complete", "total_in_system", tgaData.TotalCount, "ingested", len(leads))
	return leads, nil
}
