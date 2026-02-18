package source

import (
	"context"

	"github.com/shanehull/sourcerer/internal/model"
)

type Sourcer interface {
	Name() string
	Fetch(ctx context.Context) ([]model.Lead, error)
}
