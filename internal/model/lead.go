package model

import (
	"strings"
	"time"
)

type Lead struct {
	ABN              string // This is our Primary Key
	Name             string
	Category         string
	Sources          []string
	EntityType       string
	EntityStatus     string
	State            string
	Postcode         string
	RegistrationDate time.Time
	IsGSTRegistered  bool
	ACN              string    // ASIC number
	GSTEffectiveFrom time.Time // When GST registration became effective
	IsCurrentEntity  bool      // Whether entity is current/active
	MainTradingName  string    // How they market themselves
	Phone            string    // Contact phone
	Email            string    // Contact email
	BusinessURL      string    // Actual business website URL
	FoundAtURL       string    // URL where we found the lead (e.g., northlink.org.au/...)
	EnrichmentError  error
}

func (l *Lead) AgeYears() int {
	if l.RegistrationDate.IsZero() {
		return 0
	}
	years := time.Now().Year() - l.RegistrationDate.Year()
	if time.Now().YearDay() < l.RegistrationDate.YearDay() {
		years--
	}
	if years < 0 {
		return 0
	}
	return years
}

func (l *Lead) IsVeteran(minAge int) bool {
	return l.AgeYears() >= minAge
}

func (l *Lead) IsPrivateEntity() bool {
	lowerType := strings.ToLower(l.EntityType)

	if strings.Contains(lowerType, "public company") {
		return false
	}

	if strings.Contains(lowerType, "government") {
		return false
	}

	if strings.Contains(lowerType, "sole trader") || strings.Contains(lowerType, "individual") {
		return false
	}

	if strings.Contains(lowerType, "other incorporated entity") {
		return false
	}

	if strings.Contains(lowerType, "trust") {
		return false
	}

	return true
}

func (l *Lead) IsInvestable(allowedStates []string, allowedPostcodes []PostcodeRange) bool {
	if len(allowedStates) > 0 {
		found := false
		for _, s := range allowedStates {
			if strings.EqualFold(l.State, s) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

type PostcodeRange struct {
	Min int
	Max int
}
