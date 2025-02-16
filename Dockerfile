FROM rust:alpine AS builder

COPY Cargo.lock /build/
COPY Cargo.toml /build/
COPY src /build/src

WORKDIR /build

RUN apk add --no-cache --purge openssl-dev openssl-libs-static musl-dev libc-dev
RUN cargo build --release
RUN mkdir -p /app && mv target/release/bluepds /app/

FROM mcr.microsoft.com/cbl-mariner/distroless/minimal:2.0

COPY --from=builder /app /app
COPY default.toml /app/default.toml

#WORKDIR /home/site/wwwroot
WORKDIR /app
EXPOSE 8000

ENTRYPOINT [ "/app/bluepds" ]
