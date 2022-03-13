package zfs

import "context"

// Pools returns list of imported ZPools
func Pools(ctx context.Context) ([]*Pool, error) {
	out, err := zpool(ctx, "list", "-H", "-o", "name")
	if err != nil {
		return nil, err
	}

	pools := make([]*Pool, 0, len(out))
	for _, line := range out {
		pools = append(pools, &Pool{Name: line[0]})
	}
	return pools, nil
}

// GetPool returns ZPool by name
func GetPool(ctx context.Context, name string) (*Pool, error) {
	_, err := zpool(ctx, "list", "-H", "-o", "name", name)
	if err != nil {
		return nil, err
	}

	return &Pool{Name: name}, nil
}

// ImportPool imports ZPool
func ImportPool(ctx context.Context, name string) (*Pool, error) {
	_, err := zpool(ctx, "import", name)
	if err != nil {
		return nil, err
	}

	return &Pool{Name: name}, nil
}

// Pool represents ZPool
type Pool struct {
	Name string
}

// Export exports ZPool
func (p *Pool) Export(ctx context.Context) error {
	_, err := zpool(ctx, "export", p.Name)
	return err
}
