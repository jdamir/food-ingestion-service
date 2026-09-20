package postgres

import (
	"context"
	"food-ingestion-service/internal/domain"
	"time"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

)

type cookingRepository struct {
	db *pgxpool.Pool
}

func NewCookingRepository(db *pgxpool.Pool) domain.CookingRepository {
	return &cookingRepository{db: db}
}

func (r *cookingRepository) SaveCookingWithIngredients(ctx context.Context, userID int64, groupID string, dto *domain.CookingDTO) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var cookingID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO cooking_history (group_id, user_id, dish_item_id, dish_name, cooked_at)
		VALUES ($1, $2, $3, $4, $5) RETURNING cooking_id`,
		groupID, userID, dto.DishItemID, dto.DishName, time.Now(),
	).Scan(&cookingID)
	
	if err != nil {
		return err
	}

	batch := &pgx.Batch{}
	for _, ing := range dto.Ingredients {
		batch.Queue(`
			INSERT INTO cooking_ingredients (cooking_id, source_item_id, used_units, used_weight_grams, swiped_at)
			VALUES ($1, $2, $3, $4, $5)`,
			cookingID, 
			ing.SourceItemID, 
			ing.UsedUnits,        
			ing.UsedWeightGrams,
			time.Now(),
		)
	}


	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
