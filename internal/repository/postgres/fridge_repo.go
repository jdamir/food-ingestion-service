package postgres

import (
	"context"
	"food-ingestion-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool" 
	"fmt"
)

type fridgeRepository struct {
	db *pgxpool.Pool
}

// NewFridgeRepository инициализирует репозиторий для работы с холодильником
func NewFridgeRepository(db *pgxpool.Pool) domain.FridgeRepository {
	return &fridgeRepository{db: db}
}

// GetByGroupID извлекает все текущие остатки продуктов для конкретной домашней группы
func (r *fridgeRepository) GetByGroupID(ctx context.Context, groupID string) ([]*domain.FridgeItem, error) {
	query := `
		SELECT item_id, product_name, category, current_units, current_weight_grams, is_cooked_dish 
		FROM refrigerator_inventory 
		WHERE group_id = $1`

	rows, err := r.db.Query(ctx, query, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*domain.FridgeItem
	for rows.Next() {
		var item domain.FridgeItem
		err := rows.Scan(
			&item.ItemID, 
			&item.ProductName, 
			&item.Category, 
			&item.CurrentUnits, 
			&item.CurrentWeightGrams, 
			&item.IsCookedDish,
		)
		if err != nil {
			return nil, err
		}
		item.GroupID = groupID
		items = append(items, &item)
	}
	return items, nil
}

// UpdateStock изменяет дельту порций/веса и возвращает ошибку, если продукт с UUID не найден
func (r *fridgeRepository) UpdateStock(ctx context.Context, groupID string, itemID string, deltaUnits float64, deltaWeight float64) error {
	query := `
		UPDATE refrigerator_inventory 
		SET current_units = current_units + $1, 
		    current_weight_grams = current_weight_grams + $2, 
		    updated_at = NOW()
		WHERE group_id = $3 AND item_id = $4`
	
	res, err := r.db.Exec(ctx, query, deltaUnits, deltaWeight, groupID, itemID)
	if err != nil {
		return err
	}

	// СТРОГИЙ КОНТРОЛЬ: Если запись в базе не найдена, возвращаем ошибку и прерываем транзакцию
	if res.RowsAffected() == 0 {
		return fmt.Errorf("продукт с ID %s не найден в холодильнике группы %s", itemID, groupID)
	}

	// Автоочистка: Если товар съели или выбросили под ноль, полностью вычищаем строку
	_, _ = r.db.Exec(ctx, "DELETE FROM refrigerator_inventory WHERE current_units <= 0 AND current_weight_grams <= 0")
	return nil
}

// AddToInventory делает инсерт новой закупки или инкрементирует остатки по составному ключу
func (r *fridgeRepository) AddToInventory(ctx context.Context, groupID string, item *domain.FridgeItem) error {
	query := `
		INSERT INTO refrigerator_inventory (group_id, item_id, product_name, category, current_units, current_weight_grams, is_cooked_dish)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (group_id, item_id) 
		DO UPDATE SET 
			current_units = refrigerator_inventory.current_units + EXCLUDED.current_units, 
			current_weight_grams = refrigerator_inventory.current_weight_grams + EXCLUDED.current_weight_grams, 
			updated_at = NOW()`
	
	_, err := r.db.Exec(ctx, query, groupID, item.ItemID, item.ProductName, item.Category, item.CurrentUnits, item.CurrentWeightGrams, item.IsCookedDish)
	return err
}

