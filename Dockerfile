# Этап 1: Сборка и компиляция proto
FROM golang:1.27-alpine AS builder

# Устанавливаем protoc и системные зависимости
RUN apk add --no-cache protobuf protobuf-dev git

# Устанавливаем плагины компиляции Go gRPC
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Создаем физические папки под пакеты
RUN mkdir -p internal/delivery/grpc/pb
RUN mkdir -p internal/delivery/grpc/auth_pb

# Генерируем код трекера в папку pb
RUN protoc --go_out=. --go-grpc_out=. \
           --go_opt=Mapi/proto/tracker.proto=internal/delivery/grpc/pb \
           --go-grpc_opt=Mapi/proto/tracker.proto=internal/delivery/grpc/pb \
           api/proto/tracker.proto

# Генерируем код авторизации строго в папку auth_pb (ЖЕЛЕЗОБЕТОННЫЙ ФИКС)
RUN protoc --go_out=. --go-grpc_out=. \
           --go_opt=Mapi/proto/auth.proto=internal/delivery/grpc/auth_pb \
           --go-grpc_opt=Mapi/proto/auth.proto=internal/delivery/grpc/auth_pb \
           api/proto/auth.proto

# Компилируем сам Go бинарник приложения
RUN CGO_ENABLED=0 GOOS=linux go build -o food-app cmd/main.go

# Этап 2: Финальный легковесный Production образ
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=builder /app/food-app .
EXPOSE 8080 8081
CMD ["./food-app"]


