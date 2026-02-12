FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/build .

FROM alpine:3.20
RUN adduser -D -H -u 10001 app
# RUN mkdir -p /app/wal && chown -R 10001:10001 /app
USER app
WORKDIR /app
COPY --from=build /out/build /app/storage-importer
EXPOSE 8080
ENTRYPOINT ["/app/storage-importer"]