package source

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/shanehull/sourcerer/internal/model"
)

type CSVSource struct {
	path string
}

func NewCSVSource(path string) *CSVSource {
	return &CSVSource{path: path}
}

func (s *CSVSource) Name() string {
	return "CSV"
}

func (s *CSVSource) Fetch(ctx context.Context) ([]model.Lead, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("could not open csv: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	// Read header to find column indexes
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	cols := make(map[string]int)
	for i, name := range header {
		cols[strings.ToLower(name)] = i
	}

	var leads []model.Lead
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		get := func(key string) string {
			if idx, ok := cols[key]; ok && idx < len(record) {
				return strings.TrimSpace(record[idx])
			}
			return ""
		}

		leads = append(leads, model.Lead{
			ABN:        get("abn"),
			Name:       get("name"),
			Category:   get("category"),
			State:      strings.ToUpper(get("state")),
			FoundAtURL: get("url"),
			Sources:    []string{s.Name()},
		})
	}

	return leads, nil
}
