package enrich

import (
	"context"

	"github.com/shanehull/sourcerer/internal/model"
)

type Enricher interface {
	Enrich(ctx context.Context, lead *model.Lead) error
}
