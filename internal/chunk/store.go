package chunk

import "context"

type Store interface {
	Put(ctx context.Context, c *Chunk) error
	Get(ctx context.Context, id ChunkID) (*Chunk, error)
	Delete(ctx context.Context, id ChunkID) error
	Has(ctx context.Context, id ChunkID) (bool, error)
	List(ctx context.Context) ([]ChunkID, error)
}
