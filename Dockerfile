FROM rust:alpine AS builder

COPY .env /build/
COPY Cargo.lock /build/
COPY Cargo.toml /build/
COPY src /build/src
COPY migrations /build/migrations

WORKDIR /build

RUN apk add --no-cache --purge openssl-dev openssl-libs-static musl-dev libc-dev

# HACK: Build `sqlx-cli` and use it to set up the database. Eventually we need to move to `binstall`.
RUN cargo install sqlx-cli --no-default-features --features sqlite
RUN mkdir -p data && \
    cargo sqlx database setup

RUN cargo build --release
RUN mkdir -p /app &&                   \
    mv target/release/bluepds /app/ && \
    mv data /app/

FROM mcr.microsoft.com/cbl-mariner/distroless/minimal:2.0

COPY --from=builder /app /app
COPY default.toml /app/default.toml

#WORKDIR /home/site/wwwroot
WORKDIR /app
EXPOSE 8000

ENTRYPOINT [ "/app/bluepds" ]
