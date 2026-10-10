package kafka

import (
	"context"
	"encoding/json"
	"food-ingestion-service/internal/domain"
	"log/slog"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader     *kafka.Reader
	trackerSvc domain.FoodTrackerService
}

func NewKafkaConsumer(brokers []string, groupID string, ts domain.FoodTrackerService) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			// Слушаем массив топиков под разные события параллельно
			GroupTopics: []string{"purchase-events", "cooking-events", "consumption-events", "waste-events"},
		}),
		trackerSvc: ts,
	}
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	slog.Info("Многопоточный воркер Kafka запущен и слушает все топики событий...")
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			slog.Error("Ошибка чтения из Kafka", "error", err)
			continue
		}

		// Запускаем безопасную анонимную функцию для изоляции паник рантайма
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("КРИТИЧЕСКАЯ ПАНИКА В РАСТАРТЕ ВОРКЕРА", 
						"panic", r, 
						"topic", msg.Topic,
					)
				}
			}()

			// Распределяем логику в зависимости от того, из какого топика прилетело событие
			switch msg.Topic {
			case "purchase-events":
				slog.Debug("Сырой JSON из Кафки", "raw_body", string(msg.Value))

				var dto domain.PurchaseDTO
				if err := json.Unmarshal(msg.Value, &dto); err != nil {
					slog.Error("Ошибка парсинга JSON покупки", "error", err)
					return // Выходим из анонимной функции, цикл for идет на следующую итерацию
				}
				
				slog.Info("Воркер прочитал событие ПОКУПКИ из Kafka", 
					"receipt_no", dto.ReceiptNo, 
					"items_count", len(dto.Items), 
					"group_id", dto.GroupID,
				)
				
				// Пытаемся распарсить user_id из входящего события (в B2B чеке будет 0, от мобилки/симулятора — реальный ID)
				var targetUserID int64 = 0
				var rawMap map[string]interface{}
				if err := json.Unmarshal(msg.Value, &rawMap); err == nil {
					if uid, ok := rawMap["user_id"].(float64); ok {
						targetUserID = int64(uid)
					}
				}

				if err := c.trackerSvc.TrackPurchase(ctx, targetUserID, dto.GroupID, &dto); err != nil {
					slog.Error("Ошибка физической записи покупки в БД через воркер", "error", err)
				}

			case "cooking-events":
				var dto domain.CookingDTO
				if err := json.Unmarshal(msg.Value, &dto); err != nil {
					slog.Error("Ошибка парсинга JSON готовки", "error", err)
					return
				}

				slog.Info("Воркер прочитал событие ГОТОВКИ из Kafka", 
					"dish_name", dto.DishName, 
					"ingredients_count", len(dto.Ingredients), 
					"group_id", dto.GroupID,
				)

				// Также извлекаем user_id для готовки
				var targetUserID int64 = 0
				var rawMap map[string]interface{}
				if err := json.Unmarshal(msg.Value, &rawMap); err == nil {
					if uid, ok := rawMap["user_id"].(float64); ok {
						targetUserID = int64(uid)
					}
				}

				if err := c.trackerSvc.TrackCooking(ctx, targetUserID, dto.GroupID, &dto); err != nil {
					slog.Error("Ошибка физической записи готовки в БД через воркер", "error", err)
				}

			case "consumption-events":
				// ХАРДКОРНЫЙ ДЕБАГ: выводим сырую строку JSON, чтобы увидеть, что ТУДА реально прилетело
				slog.Info("ПОЛНЫЙ ДАМП ИЗ КАФКИ (ПОТРЕБЛЕНИЕ)", "raw_body", string(msg.Value))

				// 1. Сначала парсим в сырую мапу, чтобы увидеть точную структуру из Кафки
				var rawMap map[string]interface{}
				if err := json.Unmarshal(msg.Value, &rawMap); err != nil {
					slog.Error("Ошибка парсинга сырого JSON потребления", "error", err)
					return
				}

				// Извлекаем базовые поля
				var userID int64 = 0
				if uid, ok := rawMap["user_id"].(float64); ok {
					userID = int64(uid)
				}
				groupID, _ := rawMap["group_id"].(string)

				// Извлекаем массив продуктов динамически
				rawItems, ok := rawMap["items"].([]interface{})
				if !ok {
					slog.Error("В событии Кафки отсутствует или пуст массив 'items'")
					return
				}

				slog.Info("Воркер прочитал ПАКЕТНОЕ событие ПОТРЕБЛЕНИЯ из Kafka (Динамический парсинг)", 
					"items_count", len(rawItems),
					"group_id", groupID,
				)

				var domainItems []domain.ConsumptionItemDTO

				// 2. Вручную собираем структуры DTO, проверяя любые возможные ключи ("portions" или "units")
				for _, rawItem := range rawItems {
					itemMap, ok := rawItem.(map[string]interface{})
					if !ok {
						continue
					}

					itemID, _ := itemMap["item_id"].(string)
					
					// Проверяем оба варианта ключа порций для надежности (и portions, и units)
					var portions float64 = 0
					if p, ok := itemMap["portions"].(float64); ok {
						portions = p
					} else if u, ok := itemMap["units"].(float64); ok {
						portions = u
					}

					var weight float64 = 0
					if w, ok := itemMap["weight_grams"].(float64); ok {
						weight = w
					}

					domainItems = append(domainItems, domain.ConsumptionItemDTO{
						ItemID:      itemID,
						Units:       portions, // Маппим в поле Units, которое ждет симулятор
						WeightGrams: weight,
					})
				}

				// 3. Передаем собранный массив в атомарный сервис
				if err := c.trackerSvc.TrackBatchConsumption(ctx, userID, groupID, domainItems); err != nil {
					slog.Error("Пакетный обед не смог записаться в БД", "error", err)
				} else {
					slog.Info("Пакетный обед успешно списан и зафиксирован в истории")
				}

		case "waste-events":
	slog.Info("ПОЛНЫЙ ДАМП ИЗ КАФКИ (МУСОР)", "raw_body", string(msg.Value))

	var rawMap map[string]interface{}
	if err := json.Unmarshal(msg.Value, &rawMap); err != nil {
		slog.Error("Ошибка парсинга сырого JSON отходов", "error", err)
		return
	}

	var userID int64 = 0
	if uid, ok := rawMap["user_id"].(float64); ok {
		userID = int64(uid)
	}
	groupID, _ := rawMap["group_id"].(string)

	rawItems, ok := rawMap["items"].([]interface{})
	if !ok {
		slog.Error("В событии Кафки отсутствует или пуст массив 'items' для отходов")
		return
	}

	slog.Info("Воркер прочитал ПАКЕТНОЕ событие УТИЛИЗАЦИИ из Kafka", 
		"items_count", len(rawItems),
		"group_id", groupID,
	)

	var domainItems []domain.WasteItemDTO
	for _, rawItem := range rawItems {
		itemMap, ok := rawItem.(map[string]interface{})
		if !ok {
			continue
		}

		itemID, _ := itemMap["item_id"].(string)
		
		var portions float64 = 0
		if p, ok := itemMap["portions"].(float64); ok {
			portions = p
		} else if u, ok := itemMap["units"].(float64); ok {
			portions = u
		}

		var weight float64 = 0
		if w, ok := itemMap["weight_grams"].(float64); ok {
			weight = w
		}
		
		reason, _ := itemMap["reason"].(string)

		domainItems = append(domainItems, domain.WasteItemDTO{
			ItemID:      itemID,
			Units:       portions,
			WeightGrams: weight,
			Reason:      reason,
		})
	}

	if err := c.trackerSvc.TrackBatchWaste(ctx, userID, groupID, domainItems); err != nil {
					slog.Error("Пакетный выброс в мусор не смог записаться в БД", "error", err)
				} else {
					slog.Info("Пакетный выброс в мусор успешно зафиксирован в истории и холодильнике")
				}
			} // конец switch
		}() // конец анонимной функции дефера
	} // конец цикла for
} // конец метода Start