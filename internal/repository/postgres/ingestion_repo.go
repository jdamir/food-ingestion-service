package postgres

import (
	"context"
	"food-ingestion-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ingestionRepository struct {
	db *pgxpool.Pool
}

func NewIngestionRepository(db *pgxpool.Pool) domain.IngestionRepository {
	return &ingestionRepository{db: db}
}

// Изменено: теперь функция принимает userID и groupID напрямую как аргументы
func (r *ingestionRepository) SavePurchase(ctx context.Context, userID int64, groupID string, record *domain.PurchaseRecord) error {
	query := `
		INSERT INTO purchases_billing (group_id, user_id, receipt_no, total_price, receipt_items, purchased_at)
		VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := r.db.Exec(ctx, query,
		groupID,  // Используем аргумент функции напрямую
		userID,   // Используем аргумент функции напрямую
		record.ReceiptNo,
		record.TotalPrice,
		record.ItemsJSON,
		record.PurchasedAt,
	)
	return err
}