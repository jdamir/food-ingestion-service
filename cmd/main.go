package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"log/slog"

	// 1. Внутренние пакеты вашего проекта
	"food-ingestion-service/internal/delivery/kafka"
	grpcDelivery "food-ingestion-service/internal/delivery/grpc"
	"food-ingestion-service/internal/repository/postgres"
	"food-ingestion-service/internal/service"

	// 2. Сгенерированные внутри докера gRPC пакеты (С ЯВНЫМИ ПСЕВДОНИМАМИ)
	auth "food-ingestion-service/internal/delivery/grpc/auth_pb"
	pb "food-ingestion-service/internal/delivery/grpc/pb"         


	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	slog.Info("Запуск Food Ingestion Service...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbDSN := os.Getenv("DB_DSN")
	if dbDSN == "" {
		dbDSN = "postgres://auth_user:auth_password@postgres:5432/auth_database?sslmode=disable"
	}
	kafkaBrokerEnv := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokerEnv == "" {
		kafkaBrokerEnv = "kafka:29092"
	}
	kafkaBrokers := []string{kafkaBrokerEnv}

	authServiceAddr := os.Getenv("AUTH_SERVICE_ADDR")
	if authServiceAddr == "" {
		authServiceAddr = "auth-service:50051"
	}

	// 1. Подключение к PostgreSQL pool трекера продуктов
	dbPool, err := pgxpool.New(ctx, dbDSN)
	if err != nil {
		slog.Error("Критическая ошибка подключения к Postgres трекера", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()
	slog.Info("Успешное подключение к PostgreSQL")

	// 2. Инициализация gRPC-клиента к вашему независимому Auth-сервису
	authConn, err := grpc.DialContext(ctx, authServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Критическая ошибка: невозможно связаться с Auth-сервисом", "error", err)
		os.Exit(1)
	}
	defer authConn.Close()
	slog.Info("gRPC связь с Auth-сервисом успешно установлена", "target", authServiceAddr)

	authServiceClient := auth.NewAuthServiceClient(authConn)

	// 3. Инициализация всех репозиториев (historyRepo создается из вашего waste_consumption_repo.go)
	ingestionRepo := postgres.NewIngestionRepository(dbPool)
	fridgeRepo := postgres.NewFridgeRepository(dbPool)
	cookingRepo := postgres.NewCookingRepository(dbPool)
	historyRepo := postgres.NewHistoryRepository(dbPool)

	// 4. Инициализация продюсера Kafka
	kafkaPublisher := kafka.NewKafkaPublisher(kafkaBrokers)

	// 5. Инициализация Сервисов бизнес-логики (Service Layer)
	foodTrackerService := service.NewFoodTrackerService(
		ingestionRepo, 
		cookingRepo, 
		fridgeRepo, 
		historyRepo, // Передаем как ConsumptionRepository
		historyRepo, // Передаем как WasteRepository
	)
	foodTrackerAsyncService := service.NewFoodTrackerServiceAsync(kafkaPublisher)
	fridgeInventoryService := service.NewFridgeInventoryService(fridgeRepo)

	// 6. Старт Prometheus метрик
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		slog.Info("Prometheus метрики доступны на :8081/metrics")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			slog.Error("Ошибка HTTP сервера метрик", "error", err)
		}
	}()

	// 7. Запуск многопоточного воркера Kafka Consumer
	kafkaConsumer := kafka.NewKafkaConsumer(kafkaBrokers, "food-ingestion-group", foodTrackerService)
	go kafkaConsumer.Start(ctx)

	// 8. Настройка gRPC сервера с промышленным ИНТЕРЦЕПТОРОМ АВТОРИЗАЦИИ
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		slog.Error("Не удалось открыть TCP порт для gRPC", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcDelivery.AuthInterceptor(authServiceClient)),
	)
	
	foodTrackerServer := grpcDelivery.NewFoodTrackerServer(foodTrackerAsyncService, fridgeInventoryService)
	pb.RegisterFoodTrackerServiceServer(grpcServer, foodTrackerServer)

	reflection.Register(grpcServer)

	go func() {
		slog.Info("gRPC сервер успешно запущен на порту :8080")
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("Ошибка выполнения gRPC сервера", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("Получен сигнал выключения. Останавливаем ресурсы...")
}
