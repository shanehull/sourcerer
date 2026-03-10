package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/shanehull/sourcerer/internal/model"
)

type GooglePlacesClient struct {
	apiKey string
	logger *slog.Logger
	client *http.Client
}

func NewGooglePlacesClient(apiKey string, logger *slog.Logger) *GooglePlacesClient {
	return &GooglePlacesClient{
		apiKey: apiKey,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// searchTextRequest models the Google Places API v1 Text Search request
type searchTextRequest struct {
	TextQuery string `json:"text_query"`
}

// searchTextResponse models the Google Places API v1 Text Search response
type searchTextResponse struct {
	Places []struct {
		Name struct {
			Text string `json:"text"`
		} `json:"displayName"`
		PlaceID          string `json:"name"` // API returns full resource name like "places/ChIJxxxxxx"
		FormattedAddress string `json:"formattedAddress"`
		InternationalNum string `json:"internationalPhoneNumber"`
		Website          string `json:"websiteUri"`
		Types            []string `json:"types"`
	} `json:"places"`
}

// placeDetailsResponse models the Google Places API v1 Place Details response
type placeDetailsResponse struct {
	Name struct {
		Text string `json:"text"`
	} `json:"displayName"`
	FormattedAddress string   `json:"formattedAddress"`
	InternationalNum string   `json:"internationalPhoneNumber"`
	NationalNum      string   `json:"nationalPhoneNumber"`
	Website          string   `json:"websiteUri"`
	Types            []string `json:"types"`
	Rating           float64  `json:"rating"`
	UserRatingCount  int      `json:"userRatingCount"`
}

// Enrich enriches a lead with Google Places data
func (c *GooglePlacesClient) Enrich(ctx context.Context, lead *model.Lead) error {
	if lead.Name == "" {
		return fmt.Errorf("lead name is required for Google Places enrichment")
	}

	// Clean company name: remove common suffixes and abbreviations
	cleanName := cleanCompanyName(lead.Name)

	// Build search query with address context for better matching
	// Try with postcode first, then state, then just the name
	queries := []string{
		cleanName,
	}

	if lead.Postcode != "" {
		queries = append([]string{
			fmt.Sprintf("%s %s Australia", cleanName, lead.Postcode),
		}, queries...)
	}

	if lead.State != "" {
		queries = append([]string{
			fmt.Sprintf("%s %s Australia", cleanName, lead.State),
		}, queries...)
	}

	var placeID string
	var err error

	// Try each query until one succeeds
	for _, query := range queries {
		placeID, err = c.searchPlace(ctx, query)
		if err == nil && placeID != "" {
			break
		}
	}

	if placeID == "" {
		return fmt.Errorf("no matching place found for %s", lead.Name)
	}

	// Get detailed information about the place
	if err := c.enrichWithPlaceDetails(ctx, lead, placeID); err != nil {
		return fmt.Errorf("failed to get place details: %w", err)
	}

	return nil
}

// cleanCompanyName removes common company suffixes and abbreviations
func cleanCompanyName(name string) string {
	suffixes := []string{
		" Pty Ltd", " Pty Ltd.", " Pty Ltd,",
		" Ltd", " Ltd.", " Ltd,",
		" Inc", " Inc.", " Inc,",
		" Co.", " Co,",
		" Pty", " PTY",
		" LLC", " PLLC",
		" Corp", " Corp.",
	}

	cleaned := name
	for _, suffix := range suffixes {
		if strings.HasSuffix(cleaned, suffix) {
			cleaned = strings.TrimSuffix(cleaned, suffix)
			break
		}
	}

	return strings.TrimSpace(cleaned)
}

// searchPlace finds a place using the Google Places API v1 Text Search
func (c *GooglePlacesClient) searchPlace(ctx context.Context, query string) (string, error) {
	apiURL := "https://places.googleapis.com/v1/places:searchText"

	reqBody := searchTextRequest{
		TextQuery: query,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(jsonBody))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Goog-Api-Key", c.apiKey)
	req.Header.Set("X-Goog-FieldMask", "places.name,places.displayName,places.formattedAddress,places.internationalPhoneNumber,places.websiteUri,places.types,places.rating,places.userRatingCount")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("google places API returned status %d: %s", resp.StatusCode, string(body))
	}

	var searchResp searchTextResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	if len(searchResp.Places) == 0 {
		return "", fmt.Errorf("no results found")
	}

	// Return the first (best-match) result's place ID
	// API returns full resource name like "places/ChIJxxxxxx", extract just the ID
	placeID := searchResp.Places[0].PlaceID
	return placeID, nil
}

// enrichWithPlaceDetails fetches detailed information about a place
func (c *GooglePlacesClient) enrichWithPlaceDetails(ctx context.Context, lead *model.Lead, placeID string) error {
	// placeID is the full resource name like "places/ChIJxxxxxx"
	apiURL := fmt.Sprintf("https://places.googleapis.com/v1/%s", placeID)

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Goog-Api-Key", c.apiKey)
	req.Header.Set("X-Goog-FieldMask", "displayName,formattedAddress,internationalPhoneNumber,nationalPhoneNumber,websiteUri,types,rating,userRatingCount")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("google places API returned status %d: %s", resp.StatusCode, string(body))
	}

	var details placeDetailsResponse
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Populate lead with Google Places data
	lead.GooglePlacesID = placeID
	lead.GoogleFormattedName = details.Name.Text
	lead.GooglePhone = details.InternationalNum
	if lead.GooglePhone == "" {
		lead.GooglePhone = details.NationalNum
	}
	lead.GoogleWebsite = details.Website
	lead.GoogleFormattedAddr = details.FormattedAddress
	lead.GoogleRating = details.Rating
	lead.GoogleRatingCount = details.UserRatingCount

	// Store all business types (comma-separated)
	if len(details.Types) > 0 {
		lead.GoogleTypes = strings.Join(details.Types, ",")

		// Extract primary type from types array
		for _, t := range details.Types {
			if !isGenericType(t) {
				lead.GooglePrimaryType = t
				break
			}
		}
		// If all types are generic, use the first one
		if lead.GooglePrimaryType == "" {
			lead.GooglePrimaryType = details.Types[0]
		}
	}

	c.logger.Debug("Google Places enrichment successful",
		"name", lead.Name,
		"formatted_name", lead.GoogleFormattedName,
		"place_id", lead.GooglePlacesID,
		"rating", lead.GoogleRating,
		"rating_count", lead.GoogleRatingCount,
		"types", lead.GoogleTypes)

	return nil
}

// isGenericType filters out overly generic place types
func isGenericType(t string) bool {
	generic := map[string]bool{
		"point_of_interest": true,
		"establishment":     true,
		"general":           true,
	}
	return generic[t]
}
