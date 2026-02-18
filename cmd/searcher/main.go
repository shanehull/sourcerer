package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/shanehull/sourcerer/internal/storage"
)

func main() {
	dbPath := flag.String("db", "sourcing.duckdb", "Path to DuckDB file")
	name := flag.String("name", "", "Search by name (case-insensitive contains)")
	states := flag.String("states", "", "Filter by states (e.g. VIC,NSW)")
	minAge := flag.Int("age", 0, "Minimum business age in years")
	outPath := flag.String("out", "out/search_results.csv", "Output CSV path")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	repo, err := storage.NewDuckDBRepo(*dbPath, logger)
	if err != nil {
		logger.Error("Failed to connect to DB", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	ctx := context.Background()

	where := []string{"gst_registered = TRUE"}
	if *name != "" {
		where = append(where, fmt.Sprintf("lower(name) LIKE '%%%s%%'", strings.ToLower(*name)))
	}
	if *states != "" {
		states_split := strings.Join(strings.Split(strings.ToUpper(*states), ","), "','")
		where = append(where, fmt.Sprintf("state IN ('%s')", states_split))
	}
	if *minAge > 0 {
		cutoff := time.Now().AddDate(-*minAge, 0, 0).Format("2006-01-02")
		where = append(where, fmt.Sprintf("registration_date <= '%s'", cutoff))
	}

	query := fmt.Sprintf("COPY (SELECT * FROM leads WHERE %s) TO '%s' (HEADER, DELIMITER ',');",
		strings.Join(where, " AND "),
		*outPath,
	)

	_, err = repo.GetDB().ExecContext(ctx, query)
	if err != nil {
		logger.Error("Search failed", "error", err)
		os.Exit(1)
	}

	logger.Info("Search complete", "output", *outPath)
}
