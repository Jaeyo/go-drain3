package drain3

import "context"

type PersistenceHandler interface {
	Save(ctx context.Context, state []byte) error
	Load(ctx context.Context) ([]byte, error)
}
