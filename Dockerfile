# -------- Stage 1 --------
FROM rust:1.91 as builder

RUN apt-get update && apt-get install -y musl-tools

WORKDIR /app
COPY . .

RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --release --target x86_64-unknown-linux-musl

# -------- Stage 2 --------
FROM gcr.io/distroless/cc

WORKDIR /app

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/pocket-server .

EXPOSE 8000

CMD ["./pocket-server"]
