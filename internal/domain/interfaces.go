package domain

import (
	"context"
)

type IngestionRepository interface {
	SavePurchase(ctx context.Context, userID int64, groupID string, record *PurchaseRecord) error
}

type CookingRepository interface {
	SaveCookingWithIngredients(ctx context.Context, userID int64, groupID string, dto *CookingDTO) error
}

type FridgeRepository interface {
	GetByGroupID(ctx context.Context, groupID string) ([]*FridgeItem, error)
	// Метод возвращает строго error, под новую логику RowsAffected внутри fridge_repo.go
	UpdateStock(ctx context.Context, groupID string, itemID string, deltaUnits float64, deltaWeight float64) error
	AddToInventory(ctx context.Context, groupID string, item *FridgeItem) error
}

type ConsumptionRepository interface {
	SaveConsumption(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64) error
}

type WasteRepository interface {
	SaveWaste(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64, reason string) error
}

type FoodTrackerService interface {
	TrackPurchase(ctx context.Context, userID int64, groupID string, dto *PurchaseDTO) error
	TrackCooking(ctx context.Context, userID int64, groupID string, dto *CookingDTO) error
	TrackConsumption(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64) error
	TrackWaste(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64, reason string) error
	TrackBatchConsumption(ctx context.Context, userID int64, groupID string, items []ConsumptionItemDTO) error
	TrackBatchWaste(ctx context.Context, userID int64, groupID string, items []WasteItemDTO) error

}

type FridgeInventoryService interface {
	GetGroupInventory(ctx context.Context, groupID string) ([]*FridgeItem, error)
}

type KafkaPublisher interface {
	PublishEvent(ctx context.Context, topic string, key string, payload []byte) error
}
