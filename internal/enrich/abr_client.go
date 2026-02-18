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
	// (SearchByName logic remains unchanged as it works fine)
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
	c.logger.Info("ABR Search Results", "keyword", keyword, "found", len(leads))
	return leads, nil
}

func (c *ABRClient) Enrich(ctx context.Context, l *model.Lead) error {
	apiURL := "https://abr.business.gov.au/abrxmlsearch/ABRXMLSearch.asmx/SearchByABNv202001"

	cleanABN := strings.ReplaceAll(l.ABN, " ", "")
	params := url.Values{}
	params.Set("searchString", cleanABN)
	params.Set("includeHistoricalDetails", "N")
	params.Set("authenticationGuid", c.guid)

	fullURL := apiURL + "?" + params.Encode()
	resp, err := http.Get(fullURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	decoder := xml.NewDecoder(strings.NewReader(string(body)))
	foundData := false

	// The logic here is now "greedy". We don't care about the wrapper.
	// We just want to find these tags WHEREVER they appear in the stream.
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

			// ABR can return these tags at different depths.
			switch tagName {
			case "entitydescription":
				decoder.DecodeElement(&l.EntityType, &se)
				foundData = true
			case "entitystatuscode":
				decoder.DecodeElement(&l.EntityStatus, &se)
			case "goodsandservicestax":
				// GST element exists if active (has effectiveFrom without effectiveTo)
				var gst struct {
					EffectiveFrom string `xml:"effectiveFrom"`
					EffectiveTo   string `xml:"effectiveTo"`
				}
				if err := decoder.DecodeElement(&gst, &se); err == nil {
					if gst.EffectiveFrom != "" && gst.EffectiveTo == "0001-01-01" {
						l.IsGSTRegistered = true
					}
				}
			case "effectivefrom":
				// This catches registration dates.
				// We only take the first one we find to avoid historical noise.
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
		c.logger.Error("Enrichment missed data", "abn", cleanABN, "resp_snippet", snippet)
		return fmt.Errorf("no business data found for ABN %s", cleanABN)
	}
	return nil
}
