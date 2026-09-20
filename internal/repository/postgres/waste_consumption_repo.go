package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)


type historyRepository struct {
	db *pgxpool.Pool
}

func NewHistoryRepository(db *pgxpool.Pool) *historyRepository {
	return &historyRepository{db: db}
}


func (r *historyRepository) SaveConsumption(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64) error {
	query := `
		INSERT INTO consumption_history (user_id, group_id, item_id, units, weight_grams)
		VALUES ($1, $2, $3, $4, $5)` // Полностью убрали consumed_at из инсерта, база проставит его сама
	
	_, err := r.db.Exec(ctx, query, userID, groupID, itemID, units, weight)
	return err
}


// SaveWaste фиксирует факт утилизации испорченных продуктов в мусор
func (r *historyRepository) SaveWaste(ctx context.Context, userID int64, groupID string, itemID string, units float64, weight float64, reason string) error {
	query := `
		INSERT INTO waste_history (user_id, group_id, item_id, units, weight_grams, reason)
		VALUES ($1, $2, $3, $4, $5, $6)` // Поле wasted_at база проставит автоматически
	
	_, err := r.db.Exec(ctx, query, userID, groupID, itemID, units, weight, reason)
	return err
}

