package enrich

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shanehull/sourcerer/internal/model"
)

type ABRClient struct {
	guid   string
	logger *slog.Logger
}

func NewABRClient(guid string, logger *slog.Logger) *ABRClient {
	return &ABRClient{guid: guid, logger: logger}
}

func (c *ABRClient) SearchByName(ctx context.Context, keyword string) ([]model.Lead, error) {
	apiURL := "https://abr.business.gov.au/abrxmlsearch/ABRXMLSearch.asmx/ABRSearchByNameAdvancedSimpleProtocol2017"

	params := url.Values{}
	params.Set("authenticationGuid", c.guid)
	params.Set("name", keyword)
	params.Set("postcode", "")
	params.Set("legalName", "Y")
	params.Set("businessName", "Y")
	params.Set("tradingName", "Y")
	params.Set("NSW", "Y")
	params.Set("SA", "Y")
	params.Set("VIC", "Y")
	params.Set("QLD", "Y")
	params.Set("TAS", "Y")
	params.Set("WA", "Y")
	params.Set("NT", "Y")
	params.Set("ACT", "Y")
	params.Set("searchWidth", "Typical")
	params.Set("minimumScore", "50")
	params.Set("maxSearchResults", "200")
	params.Set("activeABNsOnly", "Y")

	resp, err := http.Get(apiURL + "?" + params.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var leads []model.Lead
	decoder := xml.NewDecoder(resp.Body)

	for {
		t, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		switch se := t.(type) {
		case xml.StartElement:
			if strings.EqualFold(se.Name.Local, "searchResultsRecord") {
				var record struct {
					ABN struct {
						IdentifierValue string `xml:"identifierValue"`
					} `xml:"ABN"`
					MainName struct {
						OrganisationName string `xml:"organisationName"`
					} `xml:"mainName"`
					BusinessName struct {
						OrganisationName string `xml:"organisationName"`
					} `xml:"businessName"`
					MainBusinessPhysicalAddress struct {
						StateCode string `xml:"stateCode"`
					} `xml:"mainBusinessPhysicalAddress"`
				}
				if err := decoder.DecodeElement(&record, &se); err == nil {
					name := record.MainName.OrganisationName
					if name == "" {
						name = record.BusinessName.OrganisationName
					}

					if record.ABN.IdentifierValue != "" {
						leads = append(leads, model.Lead{
							ABN:     record.ABN.IdentifierValue,
							Name:    name,
							State:   record.MainBusinessPhysicalAddress.StateCode,
							Sources: []string{"ABR-Search"},
						})
					}
				}
			}
		}
	}
	c.logger.Debug("ABN name lookup results", "name", keyword, "found", len(leads))
	return leads, nil
}

func (c *ABRClient) Enrich(ctx context.Context, l *model.Lead) error {
	// If no ABN, look it up by name
	if l.ABN == "" {
		results, err := c.SearchByName(ctx, l.Name)
		if err != nil || len(results) == 0 {
			return fmt.Errorf("no ABN found for %s", l.Name)
		}
		l.ABN = results[0].ABN
	}

	// Using the latest endpoint
	apiURL := "https://abr.business.gov.au/abrxmlsearch/ABRXMLSearch.asmx/SearchByABNv202001"

	params := url.Values{}
	params.Set("authenticationGuid", c.guid)
	params.Set("searchString", l.ABN)
	params.Set("includeHistoricalDetails", "N")

	fullURL := apiURL + "?" + params.Encode()
	resp, err := http.Get(fullURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	decoder := xml.NewDecoder(strings.NewReader(string(body)))
	foundData := false

	for {
		t, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		switch se := t.(type) {
		case xml.StartElement:
			tagName := strings.ToLower(se.Name.Local)
			switch tagName {
			case "entitydescription":
				decoder.DecodeElement(&l.EntityType, &se)
				foundData = true
			case "entitystatuscode":
				decoder.DecodeElement(&l.EntityStatus, &se)
			case "goodsandservicestax":
				var gst struct {
					EffectiveFrom string `xml:"effectiveFrom"`
					EffectiveTo   string `xml:"effectiveTo"`
				}
				if err := decoder.DecodeElement(&gst, &se); err == nil {
					if gst.EffectiveFrom != "" && (gst.EffectiveTo == "" || gst.EffectiveTo == "0001-01-01") {
						l.IsGSTRegistered = true
					}
				}
			case "effectivefrom":
				if l.RegistrationDate.IsZero() {
					var dateStr string
					decoder.DecodeElement(&dateStr, &se)
					if dateStr != "" && dateStr != "0001-01-01" {
						l.RegistrationDate, _ = time.Parse("2006-01-02", dateStr)
					}
				}
			case "statecode":
				if l.State == "" {
					decoder.DecodeElement(&l.State, &se)
				}
			case "postcode":
				if l.Postcode == "" {
					decoder.DecodeElement(&l.Postcode, &se)
				}
			}
		}
	}

	if !foundData {
		snippet := string(body)
		if len(snippet) > 500 {
			snippet = snippet[:500]
		}
		c.logger.Error("Enrichment missed data", "abn", l.ABN, "resp", snippet)
		return fmt.Errorf("no business data found for ABN %s", l.ABN)
	}
	return nil
}
