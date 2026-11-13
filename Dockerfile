# Use the official lightweight Go image
FROM golang:1.27-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files first (for efficient caching)
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the rest of your source code
COPY . .

# Build the Go application into an executable named 'main'
RUN go build -o main .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["./main"]