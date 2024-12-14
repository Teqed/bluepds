FROM mcr.microsoft.com/cbl-mariner/base/rust:1 as builder

COPY Cargo.lock /build/
COPY Cargo.toml /build/
COPY src /build/src

# Build the default page
WORKDIR /build

RUN cargo build --release
RUN mkdir -p /app && mv target/release/axum-app /app/

FROM mcr.microsoft.com/cbl-mariner/distroless/minimal:2.0

COPY --from=builder /app /app
COPY static /app/static
COPY templates /app/templates

#WORKDIR /home/site/wwwroot
WORKDIR /app
EXPOSE 8000

ENTRYPOINT [ "/app/axum-app" ]
