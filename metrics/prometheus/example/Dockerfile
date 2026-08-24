FROM golang:1.27-alpine AS builder
WORKDIR /src
COPY . .
RUN cd metrics/prometheus && go mod tidy && go build -o /pipeline_example ./example/

FROM alpine:3.19
COPY --from=builder /pipeline_example /pipeline_example
EXPOSE 2112
CMD ["/pipeline_example"]