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

func (s *FoodTrackerServer) TrackPurchase(ctx context.Context, req *pb.TrackPurchaseRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	var domainItems []domain.PurchaseItem
	for _, item := range req.Items {
		domainItems = append(domainItems, domain.PurchaseItem{
			ItemID:      item.ItemId,
			Name:        item.Name,
			Category:    item.Category,
			Units:       item.Portions,     // Передаем меру количества (пачки/штуки)
			WeightGrams: item.WeightGrams, // Передаем физический вес
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

func (s *FoodTrackerServer) TrackCooking(ctx context.Context, req *pb.TrackCookingRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	var domainIngredients []domain.IngredientDTO
	for _, ing := range req.Ingredients {
		domainIngredients = append(domainIngredients, domain.IngredientDTO{
			SourceItemID:    ing.SourceItemId,
			UsedUnits:       ing.UsedPortions,     // Сколько штук/пачек взяли
			UsedWeightGrams: ing.UsedWeightGrams, // Сколько грамм изъяли
		})
	}

	dto := &domain.CookingDTO{
		DishItemID:       req.DishItemId,
		DishName:         req.DishName,
		YieldUnits:       req.YieldPortions,     // Выход в штуках/порциях еды
		YieldWeightGrams: req.YieldWeightGrams, // Общий вес кастрюли
		Ingredients:      domainIngredients,
		GroupID:          groupID,
	}

	if err := s.trackerService.TrackCooking(ctx, userID, groupID, dto); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Cooking tracked successfully"}, nil
}

func (s *FoodTrackerServer) TrackConsumption(ctx context.Context, req *pb.TrackConsumptionRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	// Метод принимает и списывает параллельно и штуки, и граммы
	if err := s.trackerService.TrackConsumption(ctx, userID, groupID, req.ItemId, req.Portions, req.WeightGrams); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Consumption tracked successfully"}, nil
}

func (s *FoodTrackerServer) TrackWaste(ctx context.Context, req *pb.TrackWasteRequest) (*pb.TrackResponse, error) {
	userID := ctx.Value(UserIDKey).(int64)
	groupID := ctx.Value(GroupIDKey).(string)

	if err := s.trackerService.TrackWaste(ctx, userID, groupID, req.ItemId, req.Portions, req.WeightGrams, req.Reason); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.TrackResponse{Success: true, Message: "Waste tracked successfully"}, nil
}

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
			CurrentPortions:    item.CurrentUnits,       // Выводим остаток в штуках/порциях
			CurrentWeightGrams: item.CurrentWeightGrams, // Выводим остаток в граммах
			IsCookedDish:       item.IsCookedDish,
		})
	}
	return &pb.FridgeStateResponse{Items: pbItems}, nil
}
