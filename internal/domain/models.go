package domain

import (
	"time"
)

// PurchaseItem представляет один товар при покупке с тегами под входящий чек Postman
type PurchaseItem struct {
	ItemID      string  `json:"item_id"`
	Name        string  `json:"name"`
	Category    string  `json:"category"`
	Units       float64 `json:"portions"`     // Дробные штуки/порции (NUMERIC)
	WeightGrams float64 `json:"weight_grams"` // Физический вес в граммах (NUMERIC)
	Price       float64 `json:"price"`        // Финальная стоимость позиции для биллинга
}

// PurchaseDTO описывает структуру прилетающего чека
type PurchaseDTO struct {
	ReceiptNo   string         `json:"receipt_no"`
	TotalPrice  float64        `json:"total_price"`
	Items       []PurchaseItem `json:"items"`
	GroupID     string         `json:"group_id"`
	AccessToken string         `json:"access_token,omitempty"`
}

// PurchaseRecord используется для сериализации чека в JSONB внутри Postgres
type PurchaseRecord struct {
	ReceiptNo   string
	TotalPrice  float64
	ItemsJSON   []byte
	PurchasedAt time.Time
}

// IngredientDTO описывает списываемый при готовке ингредиент
type IngredientDTO struct {
	SourceItemID    string  `json:"source_item_id"`
	UsedUnits       float64 `json:"used_units"`
	UsedWeightGrams float64 `json:"used_weight_grams"`
}

// CookingDTO описывает результат процесса приготовления блюда
type CookingDTO struct {
	GroupID          string          `json:"group_id"`
	UserID           int64           `json:"user_id"`
	DishItemID       string          `json:"dish_item_id"`
	DishName         string          `json:"dish_name"`
	YieldUnits       float64         `json:"yield_units"`
	YieldWeightGrams float64         `json:"yield_weight_grams"`
	Ingredients      []IngredientDTO `json:"ingredients"`
}

// FridgeItem представляет срез остатков продукта в холодильнике
type FridgeItem struct {
	GroupID            string    `json:"group_id"`
	ItemID             string    `json:"item_id"`
	ProductName        string    `json:"product_name"`
	Category           string    `json:"category"`
	CurrentUnits       float64   `json:"current_units"`
	CurrentWeightGrams float64   `json:"current_weight_grams"`
	IsCookedDish       bool      `json:"is_cooked_dish"`
	ExpiresAt          time.Time `json:"expires_at"`
}

type ConsumptionItemDTO struct {
	ItemID      string  `json:"item_id"`
	Units       float64 `json:"portions"`
	WeightGrams float64 `json:"weight_grams"`
}

type WasteItemDTO struct {
	ItemID      string  `json:"item_id"`
	Units       float64 `json:"portions"` // Используем тег "portions" для матчинга с Postman
	WeightGrams float64 `json:"weight_grams"`
	Reason      string  `json:"reason"`
}