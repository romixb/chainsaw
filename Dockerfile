# Use an official Go runtime as a parent image
FROM golang:1.20 as builder

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Download and install the Go modules required for the application
RUN go mod download

# Build the Go application
RUN go build -o main .

# Use a minimal image to run the binary
FROM alpine:latest

# Set the working directory
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/chainsaw .

# Expose the port the application runs on
EXPOSE 8080

# Run the Go application
CMD ["./chainsaw"]