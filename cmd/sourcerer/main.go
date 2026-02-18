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

func main() {
	targetAge := flag.Int("age", 15, "Minimum business age")
	dbPath := flag.String("db", "out/sourcing.duckdb", "Path to DuckDB file")
	statesRaw := flag.String("states", "", "States filter (comma-separated)")
	postcodesRaw := flag.String("postcodes", "", "Postcode ranges")
	sourcesFlag := flag.String("sources", "rto,amtil,northlink,abr", "Sources to run")
	keywordsRaw := flag.String("keywords", "", "ABR search keywords")
	outDir := flag.String("outdir", "out", "Output directory for CSV and database")
	debug := flag.Bool("debug", false, "Enable debug logs")
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
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

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
	repo.Init(context.Background())

	enricher := enrich.NewABRClient(apiKey, logger)

	var sources []source.Sourcer
	for _, s := range strings.Split(*sourcesFlag, ",") {
		switch strings.TrimSpace(strings.ToLower(s)) {
		case "rto":
			sources = append(sources, source.NewRTOScraper(logger))
		case "amtil":
			sources = append(sources, source.NewAMTILScraper(logger))
		case "northlink":
			sources = append(sources, source.NewNorthLinkScraper(logger, "https://northlink.org.au/melbournes-north-food-group/manufacturer-directory/"))
			sources = append(sources, source.NewNorthLinkScraper(logger, "https://northlink.org.au/melbournes-north-food-group/service-provider-directory/"))
			sources = append(sources, source.NewNorthLinkScraper(logger, "https://northlink.org.au/melbournes-north-advanced-manufacturing-group/partner-directory/"))
		case "abr":
			kws := strings.Split(*keywordsRaw, ",")
			sources = append(sources, source.NewABRSearchSource(logger, enricher, kws))
		}
	}

	ctx := context.Background()
	// stats tracking
	stats := struct{ Found, New, Updated, Skipped, Error int }{}

	for _, src := range sources {
		srcLogger := logger.With("source", src.Name())
		leads, err := src.Fetch(ctx)
		if err != nil {
			srcLogger.Error("Fetch failed", "err", err)
			continue
		}

		for _, lead := range leads {
			stats.Found++

			// Skip leads without names
			if lead.Name == "" {
				stats.Skipped++
				continue
			}

			// Check Cache
			existing, _ := repo.GetLeadByName(ctx, lead.Name)
			if existing != nil {
				lead = *existing
			} else {
				// Enrich all leads (ABR-Search has ABN, others lookup by name)
				if err := enricher.Enrich(ctx, &lead); err != nil {
					stats.Error++
					continue
				}
				time.Sleep(500 * time.Millisecond)
			}

			// Core Filter Logic
			isVet := lead.IsVeteran(*targetAge)
			isInv := lead.IsInvestable(allowedStates, allowedPostcodes)
			isGst := lead.IsGSTRegistered
			isPrivate := lead.IsPrivateEntity()

			if isVet && isInv && isGst && isPrivate {
				isNew, err := repo.SaveLead(ctx, lead)
				if err != nil {
					stats.Error++
				} else if isNew {
					stats.New++
					srcLogger.Info("Saved new", "name", lead.Name, "age", lead.AgeYears())
				} else {
					stats.Updated++
				}
			} else {
				stats.Skipped++
				if *debug {
					srcLogger.Debug("Skipped", "name", lead.Name, "vet", isVet, "inv", isInv, "gst", isGst, "private", isPrivate)
				}
			}
		}
	}

	logger.Info("Pipeline Complete",
		"total_found", stats.Found,
		"new", stats.New,
		"updated", stats.Updated,
		"skipped", stats.Skipped,
		"errors", stats.Error)

	requestedSources := strings.Split(strings.ToUpper(*sourcesFlag), ",")
	if err := repo.ExportCSV(ctx, outPath, *targetAge, allowedStates, requestedSources); err != nil {
		logger.Error("Export failed", "err", err)
	} else {
		logger.Info("Export successful", "path", outPath)
	}
}
