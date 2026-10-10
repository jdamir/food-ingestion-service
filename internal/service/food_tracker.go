package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	
	"food-ingestion-service/internal/domain"
)

// =========================================================================
// 1. СИНХРОННЫЙ СЕРВИС: Вызывается из Kafka Consumer для физической записи в БД
// =========================================================================

type foodTrackerService struct {
	ingestionRepo   domain.IngestionRepository
	cookingRepo     domain.CookingRepository
	fridgeRepo      domain.FridgeRepository
	consumptionRepo domain.ConsumptionRepository
	wasteRepo       domain.WasteRepository
}

// NewFoodTrackerService инициализирует синхронный сервис для воркера Кафки
func NewFoodTrackerService(
	ir domain.IngestionRepository,
	cr domain.CookingRepository,
	fr domain.FridgeRepository,
	cn domain.ConsumptionRepository,
	wr domain.WasteRepository,
) domain.FoodTrackerService {
	return &foodTrackerService{
		ingestionRepo:   ir,
		cookingRepo:     cr,
		fridgeRepo:      fr,
		consumptionRepo: cn,
		wasteRepo:       wr,
	}
}

// TrackPurchase обрабатывает прилетевший чек, сохраняет биллинг и обновляет холодильник
func (s *foodTrackerService) TrackPurchase(ctx context.Context, userID int64, groupID string, dto *domain.PurchaseDTO) error {
	itemsBytes, err := json.Marshal(dto.Items)
	if err != nil {
		return err
	}

	record := &domain.PurchaseRecord{
		ReceiptNo:   dto.ReceiptNo,
		TotalPrice:  dto.TotalPrice,
		ItemsJSON:   itemsBytes,
		PurchasedAt: time.Now(),
	}

	if err := s.ingestionRepo.SavePurchase(ctx, userID, groupID, record); err != nil {
		return err
	}

	for _, item := range dto.Items {
		err := s.fridgeRepo.AddToInventory(ctx, groupID, &domain.FridgeItem{
			ItemID:             item.ItemID,
			ProductName:        item.Name,
			Category:           item.Category,
			CurrentUnits:       item.Units,
			CurrentWeightGrams: item.WeightGrams,
			IsCookedDish:       false,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// TrackCooking списывает ингредиенты и создает в холодильнике новое готовое блюдо
func (s *foodTrackerService) TrackCooking(ctx context.Context, userID int64, groupID string, dto *domain.CookingDTO) error {
	if err := s.cookingRepo.SaveCookingWithIngredients(ctx, userID, groupID, dto); err != nil {
		return err
	}

	for _, ing := range dto.Ingredients {
		if err := s.fridgeRepo.UpdateStock(ctx, groupID, ing.SourceItemID, -ing.UsedUnits, -ing.UsedWeightGrams); err != nil {
			return err
		}
	}

	return s.fridgeRepo.AddToInventory(ctx, groupID, &domain.FridgeItem{
		ItemID:             dto.DishItemID,
		ProductName:        dto.DishName,
		Category:           "Готовые блюда",
		CurrentUnits:       dto.YieldUnits,
		CurrentWeightGrams: dto.YieldWeightGrams,
		IsCookedDish:       true,
	})
}

// TrackConsumption списывает съеденное и делает запись в историю потребления только при успехе
func (s *foodTrackerService) TrackConsumption(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64) error {
	// 1. Сначала пытаемся списать. Если товара нет, метод вернет ошибку и код прервется
	if err := s.fridgeRepo.UpdateStock(ctx, groupID, itemID, -units, -weight); err != nil {
		return fmt.Errorf("отмена записи потребления: %w", err)
	}
	
	// 2. В историю пишем только тогда, когда списание из холодильника прошло успешно
	return s.consumptionRepo.SaveConsumption(ctx, userID, groupID, itemID, units, weight)
}

func (s *foodTrackerService) TrackBatchConsumption(ctx context.Context, userID int64, groupID string, items []domain.ConsumptionItemDTO) error {
	for _, item := range items {
		// Вызываем уже проверенный симулятор списания
		if err := s.fridgeRepo.UpdateStock(ctx, groupID, item.ItemID, -item.Units, -item.WeightGrams); err != nil {
			return fmt.Errorf("ошибка пакетного списания товара %s: %w", item.ItemID, err)
		}
		if err := s.consumptionRepo.SaveConsumption(ctx, userID, groupID, item.ItemID, item.Units, item.WeightGrams); err != nil {
			return fmt.Errorf("ошибка записи в историю для %s: %w", item.ItemID, err)
		}
	}
	return nil
}

// TrackWaste списывает испорченное в мусор и делает запись в историю отходов только при успехе
func (s *foodTrackerService) TrackBatchWaste(ctx context.Context, userID int64, groupID string, items []domain.WasteItemDTO) error {
	for _, item := range items {
		// Сначала строго проверяем наличие и списываем дельту из холодильника
		if err := s.fridgeRepo.UpdateStock(ctx, groupID, item.ItemID, -item.Units, -item.WeightGrams); err != nil {
			return fmt.Errorf("ошибка пакетного списания в мусор товара %s: %w", item.ItemID, err)
		}
		// Записываем лог утилизации в waste_history
		if err := s.wasteRepo.SaveWaste(ctx, userID, groupID, item.ItemID, item.Units, item.WeightGrams, item.Reason); err != nil {
			return fmt.Errorf("ошибка записи в историю мусора для %s: %w", item.ItemID, err)
		}
	}
	return nil
}

func (s *foodTrackerService) TrackWaste(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64, reason string) error {
	// Временная заглушка обратной совместимости для интерфейса
	return nil
}


// =========================================================================
// 2. АСИНХРОННЫЙ СЕРВИС: Вызывается из gRPC Handlers для отправки в Kafka
// =========================================================================

type foodTrackerAsyncService struct {
	kafkaPublisher domain.KafkaPublisher
}

// NewFoodTrackerServiceAsync инициализирует асинхронный сервис для gRPC хендлеров
func NewFoodTrackerServiceAsync(kp domain.KafkaPublisher) domain.FoodTrackerService {
	return &foodTrackerAsyncService{kafkaPublisher: kp}
}

func (s *foodTrackerAsyncService) TrackPurchase(ctx context.Context, userID int64, groupID string, dto *domain.PurchaseDTO) error {
	dto.AccessToken = ""
	dto.GroupID = groupID
	payload, err := json.Marshal(dto)
	if err != nil {
		return err
	}
	return s.kafkaPublisher.PublishEvent(ctx, "purchase-events", groupID, payload)
}

func (s *foodTrackerAsyncService) TrackCooking(ctx context.Context, userID int64, groupID string, dto *domain.CookingDTO) error {
	dto.GroupID = groupID
	dto.UserID = userID
	payload, err := json.Marshal(dto)
	if err != nil {
		return err
	}
	return s.kafkaPublisher.PublishEvent(ctx, "cooking-events", groupID, payload)
}

func (s *foodTrackerAsyncService) TrackConsumption(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64) error {
	// Метод-заглушка для удовлетворения интерфейса, теперь мы используем TrackBatchConsumption
	return nil
}


func (s *foodTrackerAsyncService) TrackBatchWaste(ctx context.Context, userID int64, groupID string, items []domain.WasteItemDTO) error {
	var jsonItems []map[string]interface{}
	for _, item := range items {
		jsonItems = append(jsonItems, map[string]interface{}{
			"item_id":      item.ItemID,
			"portions":     item.Units,
			"weight_grams": item.WeightGrams,
			"reason":       item.Reason,
		})
	}

	event := map[string]interface{}{
		"user_id":  userID,
		"group_id": groupID,
		"items":    jsonItems,
	}
	
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return s.kafkaPublisher.PublishEvent(ctx, "waste-events", groupID, payload)
}

func (s *foodTrackerAsyncService) TrackWaste(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64, reason string) error {
	return nil // Заглушка совместимости
}


func (s *foodTrackerAsyncService) TrackBatchConsumption(ctx context.Context, userID int64, groupID string, items []domain.ConsumptionItemDTO) error {
	// Вручную собираем массив мап, полностью контролируя имена ключей в JSON
	var jsonItems []map[string]interface{}
	for _, item := range items {
		jsonItems = append(jsonItems, map[string]interface{}{
			"item_id":      item.ItemID,
			"portions":     item.Units, // Явно пишем ключ "portions", который ждет Postman и воркер
			"weight_grams": item.WeightGrams,
		})
	}

	event := map[string]interface{}{
		"user_id":  userID,
		"group_id": groupID,
		"items":    jsonItems, // Передаем чистый массив простых мап
	}
	
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return s.kafkaPublisher.PublishEvent(ctx, "consumption-events", groupID, payload)
}

