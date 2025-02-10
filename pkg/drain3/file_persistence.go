package drain3

import (
	"fmt"
	"os"
	"context"
)

type FilePersistence struct {
	filePath string
}

func NewFilePersistence(filePath string) *FilePersistence {
	return &FilePersistence{filePath: filePath}
}

func (p *FilePersistence) Save(_ context.Context, state []byte) error {
	if err := os.WriteFile(p.filePath, state, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (p *FilePersistence) Load(_ context.Context) ([]byte, error) {
	if _, err := os.Stat(p.filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found: %w", err)
	}

	state, err := os.ReadFile(p.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return state, nil
}
