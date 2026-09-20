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

		// Распределяем логику в зависимости от того, из какого топика прилетело событие
		switch msg.Topic {
		case "purchase-events":
			// ВРЕМЕННЫЙ ЛОГ: выводим сырую строку JSON, прилетевшую из брокера
			slog.Debug("Сырой JSON из Кафки", "raw_body", string(msg.Value))

			var dto domain.PurchaseDTO
			if err := json.Unmarshal(msg.Value, &dto); err != nil {
				slog.Error("Ошибка парсинга JSON покупки", "error", err)
				continue
			}
			
			// ИСПРАВЛЕНО: Логируем метаданные чека и количество позиций вместо несуществующего ItemID
			slog.Info("Воркер прочитал событие ПОКУПКИ из Kafka", 
				"receipt_no", dto.ReceiptNo, 
				"items_count", len(dto.Items), 
				"group_id", dto.GroupID,
			)
			
			if err := c.trackerSvc.TrackPurchase(ctx, 42, dto.GroupID, &dto); err != nil {
				slog.Error("Ошибка физической записи покупки в БД через воркер", "error", err)
			}

		case "cooking-events":
			var dto domain.CookingDTO
			if err := json.Unmarshal(msg.Value, &dto); err != nil {
				slog.Error("Ошибка парсинга JSON готовки", "error", err)
				continue
			}

			// ДОБАВЛЕНО: Информативное логирование для контроля процесса готовки блюда
			slog.Info("Воркер прочитал событие ГОТОВКИ из Kafka", 
				"dish_name", dto.DishName, 
				"ingredients_count", len(dto.Ingredients), 
				"group_id", dto.GroupID,
			)

			if err := c.trackerSvc.TrackCooking(ctx, 42, dto.GroupID, &dto); err != nil {
				slog.Error("Ошибка физической записи готовки в БД через воркер", "error", err)
			}

				case "consumption-events":
			// Создаем локальную структуру, строго повторяющую ключи мапы продюсера
			var dto struct {
				UserID      int64   `json:"user_id"`
				GroupID     string  `json:"group_id"`
				ItemID      string  `json:"item_id"`
				Units       float64 `json:"units"`        // совпадает с "units" продюсера
				WeightGrams float64 `json:"weight_grams"` // совпадает with "weight_grams" продюсера
			}
			
			if err := json.Unmarshal(msg.Value, &dto); err != nil {
				slog.Error("Ошибка парсинга JSON потребления", "error", err)
				continue
			}

			slog.Info("Воркер прочитал событие ПОТРЕБЛЕНИЯ из Kafka", 
				"item_id", dto.ItemID, 
				"units", dto.Units, 
				"weight", dto.WeightGrams,
			)

			// Вызываем синхронный сервис и ОБЯЗАТЕЛЬНО проверяем ошибку выполнения SQL
			if err := c.trackerSvc.TrackConsumption(ctx, dto.UserID, dto.GroupID, dto.ItemID, dto.Units, dto.WeightGrams); err != nil {
				slog.Error("Ошибка физической записи потребления в БД через воркер", "error", err)
			}

				case "waste-events":
			var dto struct {
				UserID      int64   `json:"user_id"`
				GroupID     string  `json:"group_id"`
				ItemID      string  `json:"item_id"`
				Units       float64 `json:"units"`
				WeightGrams float64 `json:"weight_grams"`
				Reason      string  `json:"reason"`
			}
			
			if err := json.Unmarshal(msg.Value, &dto); err != nil {
				slog.Error("Ошибка парсинга JSON отходов", "error", err)
				continue
			}

			slog.Info("Воркер прочитал событие СПИСАНИЯ В МУСОР из Kafka", 
				"item_id", dto.ItemID, 
				"units", dto.Units, 
				"weight", dto.WeightGrams,
				"reason", dto.Reason,
			)

			// ИСПРАВЛЕНО: Вызываем строго метод TrackWaste (никаких покупок и AddToInventory!)
			if err := c.trackerSvc.TrackWaste(ctx, dto.UserID, dto.GroupID, dto.ItemID, dto.Units, dto.WeightGrams, dto.Reason); err != nil {
				slog.Error("Ошибка физической записи отходов в БД через воркер", "error", err)
			}


		}
	}
}
