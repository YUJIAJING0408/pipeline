FROM golang:1.27-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN cd metrics/prometheus && go mod tidy && go build -o /pipeline_example ./example/

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /pipeline_example /pipeline_example
EXPOSE 2112
CMD ["/pipeline_example"]