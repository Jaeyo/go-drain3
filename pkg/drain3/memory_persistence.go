package drain3

import "context"

type MemoryPersistence struct {
	State []byte
}

func NewMemoryPersistence() *MemoryPersistence {
	return &MemoryPersistence{}
}

func (p *MemoryPersistence) Save(_ context.Context, state []byte) error {
	p.State = state
	return nil
}

func (p *MemoryPersistence) Load(_ context.Context) ([]byte, error) {
	return p.State, nil
}
