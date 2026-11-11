// Package app — сценарии waiting room.
package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/queue/api"
	"github.com/nikolaysavelev/soldout/internal/queue/domain"
)

// ErrNotFound — допуск не найден в хранилище.
var ErrNotFound = errors.New("queue: допуск не найден")

// Cache — хранилище токенов допуска (Valkey) с TTL.
type Cache interface {
	Set(ctx context.Context, a domain.Admission, ttl time.Duration) error
	Get(ctx context.Context, token string) (domain.Admission, error)
}

// Service — реализация api.Service.
type Service struct {
	cache   Cache
	catalog catalogapi.Service
	ttl     time.Duration
	now     func() time.Time
}

// New собирает сервис.
func New(cache Cache, catalog catalogapi.Service, ttl time.Duration) *Service {
	return &Service{cache: cache, catalog: catalog, ttl: ttl, now: time.Now}
}

var _ api.Service = (*Service)(nil)

// Join — на L1 допуск выдаётся сразу; мероприятие должно существовать.
func (s *Service) Join(ctx context.Context, eventID, userID uuid.UUID) (api.Admission, error) {
	if _, err := s.catalog.GetEvent(ctx, eventID); err != nil {
		return api.Admission{}, err
	}
	if userID == uuid.Nil {
		return api.Admission{}, fmt.Errorf("%w: пустой user_id", api.ErrAdmissionInvalid)
	}
	a := domain.NewAdmission(eventID, userID, s.now(), s.ttl)
	if err := s.cache.Set(ctx, a, s.ttl); err != nil {
		return api.Admission{}, err
	}
	return a.ToAPI(), nil
}

// Validate проверяет допуск в Valkey — единственном хранилище допусков.
func (s *Service) Validate(ctx context.Context, token string, eventID, userID uuid.UUID) error {
	if token == "" {
		return api.ErrAdmissionInvalid
	}
	a, err := s.cache.Get(ctx, token)
	if errors.Is(err, ErrNotFound) {
		return api.ErrAdmissionInvalid
	}
	if err != nil {
		return err
	}
	if !a.ValidFor(eventID, userID, s.now()) {
		return api.ErrAdmissionInvalid
	}
	return nil
}
