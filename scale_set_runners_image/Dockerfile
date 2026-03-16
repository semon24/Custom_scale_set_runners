FROM golang:1.25.6 AS builder
WORKDIR /app
COPY . .
RUN go build -o dockerscaleset .

FROM builder AS final
COPY --from=builder /app/dockerscaleset /usr/local/bin/
ENTRYPOINT ["dockerscaleset"]