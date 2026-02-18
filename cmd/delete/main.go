package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/shanehull/sourcerer/internal/storage"
)

func main() {
	name := flag.String("name", "", "Lead name to delete")
	abn := flag.String("abn", "", "Lead ABN to delete")
	age := flag.Int("age", 0, "Lead age in years (for matching)")
	source := flag.String("source", "", "Lead source (for matching)")
	entityType := flag.String("entity-type", "", "Lead entity type (for matching)")
	dbPath := flag.String("db", "out/sourcing.duckdb", "Path to DuckDB file")
	flag.Parse()

	// Check if at least one filter flag was explicitly provided
	hasFilters := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "name" || f.Name == "abn" || f.Name == "age" || f.Name == "source" || f.Name == "entity-type" {
			hasFilters = true
		}
	})
	if !hasFilters {
		fmt.Fprintf(os.Stderr, "Error: at least one filter is required (-name, -abn, -age, or -source)\n")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	repo, err := storage.NewDuckDBRepo(*dbPath, logger)
	if err != nil {
		logger.Error("DB connection failed", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	ctx := context.Background()

	// Build filter conditions - check which flags were provided
	filters := map[string]interface{}{}
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "name":
			filters["name"] = *name
		case "abn":
			filters["abn"] = *abn
		case "age":
			if *age > 0 {
				filters["age"] = *age
			}
		case "source":
			filters["source"] = *source
		case "entity-type":
			filters["entity_type"] = *entityType
		}
	})

	// Confirm deletion
	fmt.Println("\nDelete with filters:")
	for k, v := range filters {
		fmt.Printf("  %s: %v\n", k, v)
	}
	fmt.Print("\nAre you sure? (yes/no): ")
	
	reader := bufio.NewReader(os.Stdin)
	response, _ := reader.ReadString('\n')
	response = strings.ToLower(strings.TrimSpace(response))
	if response != "yes" && response != "y" {
		fmt.Println("Cancelled.")
		os.Exit(0)
	}

	rowsDeleted, err := repo.DeleteLeadByFilters(ctx, filters)
	if err != nil {
		logger.Error("Delete failed", "filters", filters, "err", err)
		os.Exit(1)
	}

	if rowsDeleted == 0 {
		logger.Warn("No records matched the filters", "filters", filters)
	} else {
		logger.Info("Deleted successfully", "filters", filters, "rows_deleted", rowsDeleted)
	}
}
