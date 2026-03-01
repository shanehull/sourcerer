package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shanehull/sourcerer/internal/enrich"
	"github.com/shanehull/sourcerer/internal/model"
	"github.com/shanehull/sourcerer/internal/source"
	"github.com/shanehull/sourcerer/internal/storage"
)

func generateCSVPath(sources, states string, age int, outDir string) string {
	filename := fmt.Sprintf("sources-%s-min-age-%d", sources, age)
	if states != "" {
		statesHyphenated := strings.ReplaceAll(states, ",", "-")
		filename += "-states-" + statesHyphenated
	}
	filename += "-" + time.Now().Format("20060102") + ".csv"
	return filepath.Join(outDir, filename)
}

type stats struct {
	Found, New, Updated, Skipped, Selected, Error int
	mu                                            sync.Mutex
}

func (s *stats) incr(field string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch field {
	case "Found":
		s.Found += n
	case "New":
		s.New += n
	case "Updated":
		s.Updated += n
	case "Skipped":
		s.Skipped += n
	case "Selected":
		s.Selected += n
	case "Error":
		s.Error += n
	}
}

func main() {
	targetAge := flag.Int("age", 15, "Minimum business age")
	dbPath := flag.String("db", "out/sourcing.duckdb", "Path to DuckDB file")
	statesRaw := flag.String("states", "", "States filter (comma-separated)")
	postcodesRaw := flag.String("postcodes", "", "Postcode ranges")
	sourcesFlag := flag.String("sources", "rto,amtil,semma,northlink,abr", "Sources to run")
	keywordsRaw := flag.String("keywords", "", "ABR search keywords")
	outDir := flag.String("outdir", "out", "Output directory for CSV and database")
	debug := flag.Bool("debug", false, "Enable debug logs")
	exportOnly := flag.Bool("export-only", false, "Only export existing data, skip scraping")
	flag.Parse()

	// Ensure output directory exists
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	outPath := generateCSVPath(
		strings.ReplaceAll(*sourcesFlag, ",", "-"),
		strings.ToLower(*statesRaw),
		*targetAge,
		*outDir,
	)

	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	var allowedStates []string
	if *statesRaw != "" {
		allowedStates = strings.Split(strings.ToUpper(*statesRaw), ",")
	}

	var allowedPostcodes []model.PostcodeRange
	if *postcodesRaw != "" {
		for _, r := range strings.Split(*postcodesRaw, ",") {
			before, after, found := strings.Cut(r, "-")
			if found {
				min, _ := strconv.Atoi(before)
				max, _ := strconv.Atoi(after)
				allowedPostcodes = append(allowedPostcodes, model.PostcodeRange{Min: min, Max: max})
			}
		}
	}

	apiKey := os.Getenv("ABR_GUID")
	if apiKey == "" {
		logger.Error("ABR_GUID environment variable not set")
		os.Exit(1)
	}

	repo, err := storage.NewDuckDBRepo(*dbPath, logger)
	if err != nil {
		logger.Error("DB connection failed", "err", err)
		os.Exit(1)
	}
	defer repo.Close()
	ctx := context.Background()
	repo.Init(ctx)

	enricher := enrich.NewABRClient(apiKey, logger)

	// Export-only mode: skip scraping and go straight to export
	if *exportOnly {
		logger.Info("Export-only mode enabled, exporting existing data")
		requestedSources := strings.Split(strings.ToUpper(*sourcesFlag), ",")
		if err := repo.ExportCSV(ctx, outPath, *targetAge, allowedStates, requestedSources); err != nil {
			logger.Error("Export failed", "err", err)
		} else {
			logger.Info("Export successful", "path", outPath)
		}
		return
	}

	var sources []source.Sourcer
	for _, s := range strings.Split(*sourcesFlag, ",") {
		s := strings.TrimSpace(strings.ToLower(s))
		switch s {
		case "rto":
			srcLogger := logger.With("source", "RTO")
			sources = append(sources, source.NewRTOScraper(srcLogger))
		case "amtil":
			srcLogger := logger.With("source", "AMTIL")
			sources = append(sources, source.NewAMTILScraper(srcLogger))
		case "semma":
			srcLogger := logger.With("source", "SEMMA")
			sources = append(sources, source.NewSEMMAScraper(srcLogger))
		case "northlink":
			for _, spec := range []struct {
				url  string
				cat  string
				name string
			}{
				{"https://northlink.org.au/melbournes-north-food-group/manufacturer-directory/", "Manufacturing", "NorthLink-FoodMfg"},
				{"https://northlink.org.au/melbournes-north-food-group/service-provider-directory/", "Service Provider", "NorthLink-FoodSvc"},
				{"https://northlink.org.au/melbournes-north-advanced-manufacturing-group/partner-directory/", "Manufacturing", "NorthLink-MfgPartner"},
			} {
				srcLogger := logger.With("source", spec.name)
				sources = append(sources, source.NewNorthLinkScraper(srcLogger, spec.url, spec.cat, spec.name))
			}
		case "abr":
			srcLogger := logger.With("source", "ABR")
			kws := strings.Split(*keywordsRaw, ",")
			sources = append(sources, source.NewABRSearchSource(srcLogger, enricher, kws))
		}
	}

	// Fetch all sources concurrently
	s := &stats{}
	type fetchResult struct {
		source source.Sourcer
		leads  []model.Lead
		err    error
	}
	resultsChan := make(chan fetchResult, len(sources))

	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)
		go func(src source.Sourcer) {
			defer wg.Done()
			srcLogger := logger.With("source", src.Name())
			leads, err := src.Fetch(ctx)
			if err != nil {
				srcLogger.Error("Fetch failed", "err", err)
				resultsChan <- fetchResult{source: src, err: err}
				return
			}
			resultsChan <- fetchResult{source: src, leads: leads}
		}(src)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Process results as they come in
	for result := range resultsChan {
		if result.err != nil {
			continue
		}

		srcLogger := logger.With("source", result.source.Name())
		for _, lead := range result.leads {
			s.incr("Found", 1)

			// Skip leads without names
			if lead.Name == "" {
				s.incr("Skipped", 1)
				continue
			}

			// Check Cache
			existing, _ := repo.GetLeadByName(ctx, lead.Name)
			if existing != nil {
				lead = *existing
			} else {
				// Enrich all leads (ABR-Search has ABN, others lookup by name)
				if err := enricher.Enrich(ctx, &lead); err != nil {
					srcLogger.Error("Enrichment failed", "name", lead.Name, "abn", lead.ABN, "err", err)
					s.incr("Error", 1)
					continue
				}
			}

			// Core Filter Logic
			isVet := lead.IsVeteran(*targetAge)
			isInv := lead.IsInvestable(allowedStates, allowedPostcodes)
			isGst := lead.IsGSTRegistered
			isPrivate := lead.IsPrivateEntity()

			if isVet && isInv && isGst && isPrivate {
				isNew, err := repo.SaveLead(ctx, lead)
				if err != nil {
					srcLogger.Error("Save failed", "name", lead.Name, "err", err)
					s.incr("Error", 1)
				} else {
					s.incr("Selected", 1)
					if isNew {
						s.incr("New", 1)
						srcLogger.Info("Saved new", "name", lead.Name, "age", lead.AgeYears())
					} else {
						s.incr("Updated", 1)
					}
				}
			} else {
				s.incr("Skipped", 1)
				if *debug {
					srcLogger.Debug("Skipped", "name", lead.Name, "vet", isVet, "inv", isInv, "gst", isGst, "private", isPrivate)
				}
			}
		}
	}

	logger.Info("Pipeline Complete",
		"total_found", s.Found,
		"selected", s.Selected,
		"new", s.New,
		"updated", s.Updated,
		"skipped", s.Skipped,
		"errors", s.Error)

	requestedSources := strings.Split(strings.ToUpper(*sourcesFlag), ",")
	if err := repo.ExportCSV(ctx, outPath, *targetAge, allowedStates, requestedSources); err != nil {
		logger.Error("Export failed", "err", err)
	} else {
		logger.Info("Export successful", "path", outPath)
	}
}
