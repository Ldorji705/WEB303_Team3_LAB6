FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "$SERVICE" && CGO_ENABLED=0 GOOS=linux go build -o /app ./${SERVICE}

FROM alpine:3.22

RUN adduser -D -H appuser
COPY --from=build /app /app

USER appuser
ENTRYPOINT ["/app"]
