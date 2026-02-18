package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/shanehull/sourcerer/internal/model"
)

type DuckDBRepo struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewDuckDBRepo(path string, logger *slog.Logger) (*DuckDBRepo, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	return &DuckDBRepo{db: db, logger: logger}, nil
}

func (r *DuckDBRepo) Init(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS leads (
		abn TEXT PRIMARY KEY,
		name TEXT,
		category TEXT,
		sources TEXT,
		entity_type TEXT,
		entity_status TEXT,
		state TEXT,
		postcode TEXT,
		registration_date TIMESTAMP,
		gst_registered BOOLEAN,
		found_at_url TEXT,
		updated_at TIMESTAMP
	);`
	_, err := r.db.ExecContext(ctx, query)
	return err
}

// GetLeadByName checks if we already have an enriched lead by its name
func (r *DuckDBRepo) GetLeadByName(ctx context.Context, name string) (*model.Lead, error) {
	query := `SELECT abn, name, category, entity_type, state, registration_date, gst_registered 
	          FROM leads WHERE lower(name) = ? LIMIT 1`
	row := r.db.QueryRowContext(ctx, query, strings.ToLower(name))

	var l model.Lead
	err := row.Scan(&l.ABN, &l.Name, &l.Category, &l.EntityType, &l.State, &l.RegistrationDate, &l.IsGSTRegistered)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &l, nil
}

func (r *DuckDBRepo) SaveLead(ctx context.Context, l model.Lead) (bool, error) {
	var exists bool
	_ = r.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM leads WHERE abn = ?)", l.ABN).Scan(&exists)

	sourceStr := strings.Join(l.Sources, ",")
	query := `
	INSERT INTO leads (abn, name, category, sources, entity_type, entity_status, state, postcode, registration_date, gst_registered, found_at_url, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (abn) DO UPDATE SET
		sources = CASE WHEN CONTAINS(leads.sources, EXCLUDED.sources) THEN leads.sources ELSE leads.sources || ',' || EXCLUDED.sources END,
		updated_at = EXCLUDED.updated_at;`

	_, err := r.db.ExecContext(ctx, query, l.ABN, l.Name, l.Category, sourceStr, l.EntityType, l.EntityStatus, l.State, l.Postcode, l.RegistrationDate, l.IsGSTRegistered, l.FoundAtURL, time.Now())
	return !exists, err
}

func (r *DuckDBRepo) ExportCSV(ctx context.Context, path string, minAge int, states []string, sources []string) error {
	cutoff := time.Now().AddDate(-minAge, 0, 0).Format("2006-01-02")
	filters := []string{
		fmt.Sprintf("registration_date <= '%s'", cutoff),
		"gst_registered = TRUE",
		"lower(entity_type) NOT LIKE '%public company%'",
		"lower(entity_type) NOT LIKE '%government%'",
	}

	if len(states) > 0 && states[0] != "" {
		filters = append(filters, fmt.Sprintf("state IN ('%s')", strings.Join(states, "','")))
	}

	if len(sources) > 0 && sources[0] != "" {
		var srcConds []string
		for _, s := range sources {
			srcConds = append(srcConds, fmt.Sprintf("contains(upper(sources), '%s')", strings.ToUpper(s)))
		}
		filters = append(filters, "("+strings.Join(srcConds, " OR ")+")")
	}

	query := fmt.Sprintf(`
		COPY (
			SELECT abn, name, category, sources, state, postcode, registration_date, found_at_url 
			FROM leads 
			WHERE %s 
			ORDER BY registration_date ASC
		) TO '%s' (HEADER, DELIMITER ',');`, strings.Join(filters, " AND "), path)

	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *DuckDBRepo) DeleteLeadByName(ctx context.Context, name string) error {
	query := `DELETE FROM leads WHERE lower(name) = ?`
	_, err := r.db.ExecContext(ctx, query, strings.ToLower(name))
	return err
}

func (r *DuckDBRepo) DeleteLeadByFilters(ctx context.Context, filters map[string]interface{}) error {
	var conditions []string
	var args []interface{}

	if name, ok := filters["name"].(string); ok {
		conditions = append(conditions, "lower(name) = ?")
		args = append(args, strings.ToLower(name))
	}

	if abn, ok := filters["abn"].(string); ok {
		conditions = append(conditions, "abn = ?")
		args = append(args, abn)
	}

	if age, ok := filters["age"].(int); ok && age > 0 {
		cutoff := time.Now().AddDate(-age, 0, 0)
		nextYear := time.Now().AddDate(-age+1, 0, 0)
		conditions = append(conditions, "registration_date >= ? AND registration_date < ?")
		args = append(args, cutoff, nextYear)
	}

	if source, ok := filters["source"].(string); ok && source != "" {
		conditions = append(conditions, fmt.Sprintf("contains(upper(sources), '%s')", strings.ToUpper(source)))
	}

	if len(conditions) == 0 {
		return fmt.Errorf("no filters provided")
	}

	query := fmt.Sprintf("DELETE FROM leads WHERE %s", strings.Join(conditions, " AND "))
	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}

func (r *DuckDBRepo) Close() error {
	return r.db.Close()
}

func (r *DuckDBRepo) GetDB() *sql.DB {
	return r.db
}
