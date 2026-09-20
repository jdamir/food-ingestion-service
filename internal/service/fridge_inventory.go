package service

import (
	"context"
	"food-ingestion-service/internal/domain"
)

type fridgeInventoryService struct {
	fridgeRepo domain.FridgeRepository
}

func NewFridgeInventoryService(repo domain.FridgeRepository) domain.FridgeInventoryService {
	return &fridgeInventoryService{fridgeRepo: repo}
}

func (s *fridgeInventoryService) GetGroupInventory(ctx context.Context, groupID string) ([]*domain.FridgeItem, error) {
	return s.fridgeRepo.GetByGroupID(ctx, groupID)
}
