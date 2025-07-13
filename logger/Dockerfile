# Build stage
FROM golang:1.24.5-bullseye AS builder

WORKDIR /app

# Copy only go.mod first
COPY go.mod ./

# (Optional) if you have go.sum in the future, then:
# COPY go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the Go app from root-level main.go
RUN go build -o media_app .

# Final stage
FROM debian:bullseye-slim

# Install certificates (needed for HTTPS in many apps)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built binary from the builder
COPY --from=builder /app/media_app .

# Run the binary
CMD ["./media_app"]
