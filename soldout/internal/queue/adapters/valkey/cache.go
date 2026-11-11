// Package valkey — источник истины для допусков: ключ admission:{token}, значение JSON, TTL = срок допуска.
package valkey

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/nikolaysavelev/soldout/internal/queue/app"
	"github.com/nikolaysavelev/soldout/internal/queue/domain"
)

// Cache — Valkey-хранилище допусков, реализация app.Cache.
type Cache struct{ client vk.Client }

// New создаёт кэш.
func New(client vk.Client) *Cache { return &Cache{client: client} }

func key(token string) string { return "admission:" + token }

// Set — сохранить допуск с TTL.
func (c *Cache) Set(ctx context.Context, a domain.Admission, ttl time.Duration) error {
	b, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("queue/valkey: marshal: %w", err)
	}
	cmd := c.client.B().Set().Key(key(a.Token)).Value(string(b)).Px(ttl).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("queue/valkey: set: %w", err)
	}
	return nil
}

// Get — допуск из Valkey; app.ErrNotFound при отсутствии или истечении TTL.
func (c *Cache) Get(ctx context.Context, token string) (domain.Admission, error) {
	var a domain.Admission
	res := c.client.Do(ctx, c.client.B().Get().Key(key(token)).Build())
	if err := res.Error(); err != nil {
		if vk.IsValkeyNil(err) {
			return a, app.ErrNotFound
		}
		return a, fmt.Errorf("queue/valkey: get: %w", err)
	}
	raw, err := res.ToString()
	if err != nil {
		return a, fmt.Errorf("queue/valkey: read: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		return a, fmt.Errorf("queue/valkey: unmarshal: %w", err)
	}
	return a, nil
}
