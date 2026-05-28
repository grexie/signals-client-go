FROM golang:1.22-bookworm AS build

WORKDIR /src
COPY . .
WORKDIR /src/examples/signalsbot
RUN go mod download
RUN go build -o /out/signalsbot .

FROM debian:bookworm-slim
RUN useradd -r -u 10001 signalsbot
WORKDIR /app
COPY --from=build /out/signalsbot /usr/local/bin/signalsbot
RUN mkdir -p /data && chown -R signalsbot:signalsbot /data
USER signalsbot
ENV SIGNALS_DB_PATH=/data/signalsbot.db
ENTRYPOINT ["/usr/local/bin/signalsbot"]
CMD ["papertrader"]
