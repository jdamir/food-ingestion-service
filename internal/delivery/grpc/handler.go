package grpc

import (
	"context"
	"food-ingestion-service/internal/delivery/grpc/pb"
	"food-ingestion-service/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


type FoodTrackerServer struct {
	pb.UnimplementedFoodTrackerServiceServer
	trackerService domain.FoodTrackerService
	fridgeService  domain.FridgeInventoryService
}

func NewFoodTrackerServer(ts domain.FoodTrackerService, fs domain.FridgeInventoryService) *FoodTrackerServer {
	return &FoodTrackerServer{trackerService: ts, fridgeService: fs}
}

// 1. ПОКУПКА (TrackPurchase)
func (s *FoodTrackerServer) TrackPurchase(ctx context.Context, req *pb.TrackPurchaseRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	var domainItems []domain.PurchaseItem
	for _, item := range req.Items {
		domainItems = append(domainItems, domain.PurchaseItem{
			ItemID:      item.ItemId,
			Name:        item.Name,
			Category:    item.Category,
			Units:       item.Portions,    
			WeightGrams: item.WeightGrams,
			Price:       item.Price,
		})
	}

	dto := &domain.PurchaseDTO{
		ReceiptNo:  req.ReceiptNo,
		TotalPrice: req.TotalPrice,
		Items:      domainItems,
		GroupID:    groupID,
	}

	if err := s.trackerService.TrackPurchase(ctx, userID, groupID, dto); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Purchase tracked successfully"}, nil
}

// 2. ГОТОВКА (TrackCooking)
func (s *FoodTrackerServer) TrackCooking(ctx context.Context, req *pb.TrackCookingRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	var domainIngredients []domain.IngredientDTO
	for _, ing := range req.Ingredients {
		domainIngredients = append(domainIngredients, domain.IngredientDTO{
			SourceItemID:    ing.SourceItemId,
			UsedUnits:       ing.UsedPortions,     
			UsedWeightGrams: ing.UsedWeightGrams, 
		})
	}

	dto := &domain.CookingDTO{
		DishItemID:       req.DishItemId,
		DishName:         req.DishName,
		YieldUnits:       req.YieldPortions,     
		YieldWeightGrams: req.YieldWeightGrams, 
		Ingredients:      domainIngredients,
		GroupID:          groupID,
	}

	if err := s.trackerService.TrackCooking(ctx, userID, groupID, dto); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Cooking tracked successfully"}, nil
}

// 3. ПОТРЕБЛЕНИЕ / ОБЕД (TrackConsumption - Пакетный метод)
func (s *FoodTrackerServer) TrackConsumption(ctx context.Context, req *pb.TrackConsumptionRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	var domainItems []domain.ConsumptionItemDTO
	for _, item := range req.Items {
		domainItems = append(domainItems, domain.ConsumptionItemDTO{
			ItemID:      item.ItemId,
			Units:       item.Portions,
			WeightGrams: item.WeightGrams,
		})
	}

	if err := s.trackerService.TrackBatchConsumption(ctx, userID, groupID, domainItems); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Batch consumption tracked successfully"}, nil
}

// 4. СПИСАНИЕ В МУСОР (TrackWaste)
func (s *FoodTrackerServer) TrackWaste(ctx context.Context, req *pb.TrackWasteRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	var domainItems []domain.WasteItemDTO
	for _, item := range req.Items {
		domainItems = append(domainItems, domain.WasteItemDTO{
			ItemID:      item.ItemId,
			Units:       item.Portions,
			WeightGrams: item.WeightGrams,
			Reason:      item.Reason,
		})
	}

	if err := s.trackerService.TrackBatchWaste(ctx, userID, groupID, domainItems); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Batch waste tracked successfully"}, nil
}

// 5. СОСТОЯНИЕ ХОЛОДИЛЬНИКА (GetFridgeState)
func (s *FoodTrackerServer) GetFridgeState(ctx context.Context, req *pb.FridgeStateRequest) (*pb.FridgeStateResponse, error) {
	groupID := ctx.Value(GroupIDKey).(string)

	items, err := s.fridgeService.GetGroupInventory(ctx, groupID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ошибка получения инвентаря: %v", err)
	}

	var pbItems []*pb.FridgeItem
	for _, item := range items {
		pbItems = append(pbItems, &pb.FridgeItem{
			ItemId:             item.ItemID,
			ProductName:        item.ProductName,
			Category:           item.Category,
			CurrentPortions:    item.CurrentUnits,       
			CurrentWeightGrams: item.CurrentWeightGrams, 
			IsCookedDish:       item.IsCookedDish,
		})
	}
	return &pb.FridgeStateResponse{Items: pbItems}, nil
}
