-- deployment/postgres/init.sql

-- 1. ПОКУПКИ (Сжатые в JSONB для биллинга)
CREATE TABLE IF NOT EXISTS purchases_billing (
    purchase_id BIGSERIAL PRIMARY KEY,
    group_id UUID NOT NULL,
    user_id INT NOT NULL,
    receipt_no VARCHAR(50),
    total_price NUMERIC(10,2),
    receipt_items JSONB NOT NULL, 
    purchased_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_purchases_group ON purchases_billing(group_id);

-- 2. ТЕКУЩЕЕ СОСТОЯНИЕ ХОЛОДИЛЬНИКА
-- Создаем основную таблицу инвентаря холодильника
CREATE TABLE refrigerator_inventory (
    group_id UUID NOT NULL,
    item_id UUID NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    
-- РАЗДЕЛЯЕМ НА ДВА СТОЛБЦА (NUMERIC позволяет хранить дробные значения типа 0.5):
    current_units NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    current_weight_grams NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    
    is_cooked_dish BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (group_id, item_id)
);


-- =========================================================================
-- 3. ИСТОРИЯ ГОТОВКИ (Заголовок события)
-- =========================================================================
CREATE TABLE IF NOT EXISTS cooking_history (
    cooking_id BIGSERIAL PRIMARY KEY,
    group_id UUID NOT NULL,
    user_id INT NOT NULL,
    dish_item_id UUID NOT NULL, -- Сквозной ID получившегося блюда
    dish_name VARCHAR(150),
    cooked_at TIMESTAMP NOT NULL
);

-- Ингредиенты (Детали готовки)
CREATE TABLE IF NOT EXISTS cooking_ingredients (
    id BIGSERIAL PRIMARY KEY,
    -- Каскадно удаляет ингредиенты, если удалена запись готовки
    cooking_id BIGINT NOT NULL REFERENCES cooking_history(cooking_id) ON DELETE CASCADE,
    source_item_id UUID NOT NULL,       -- Что именно списали из холодильника
    used_units NUMERIC(10,2) NOT NULL,  -- Количество штук/порций
    used_weight_grams NUMERIC(10,2) NOT NULL DEFAULT 0.00, -- ИСПРАВЛЕНО: Добавлено недостающее поле для веса в граммах!     
    swiped_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cooking_ingredients_id ON cooking_ingredients(cooking_id);
CREATE INDEX IF NOT EXISTS idx_cooking_ingredients_source ON cooking_ingredients(source_item_id);


-- 4. ИСТОРИЯ ПОТРЕБЛЕНИЯ
CREATE TABLE consumption_history (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    group_id UUID NOT NULL,
    item_id UUID NOT NULL,
    units NUMERIC(10, 2) NOT NULL,        -- Количество съеденных штук/порций
    weight_grams NUMERIC(10, 2) NOT NULL, -- Вес съеденного в граммах
    consumed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. ИСТОРИЯ ВЫБРОСА В МУСОР
CREATE TABLE waste_history (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    group_id UUID NOT NULL,
    item_id UUID NOT NULL,
    units NUMERIC(10, 2) NOT NULL,        -- Количество выброшенных штук/порций
    weight_grams NUMERIC(10, 2) NOT NULL, -- Вес выброшенного в граммах
    reason VARCHAR(255) NOT NULL,         -- Причина (например, "Испортилось")
    wasted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);