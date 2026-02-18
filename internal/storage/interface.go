package storage

import (
	"context"

	"github.com/shanehull/sourcerer/internal/model"
)

type Repository interface {
	Init(ctx context.Context) error
	SaveLead(ctx context.Context, lead model.Lead) error
	ExportCSV(ctx context.Context, path string) error
	Close() error
}
