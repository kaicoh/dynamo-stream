FROM public.ecr.aws/docker/library/rust:slim-bullseye as builder

RUN rustup target add x86_64-unknown-linux-musl
RUN apt update -y && \
    apt upgrade -y && \
    apt install -y \
        musl-tools \
        musl-dev \
        build-essential \
        gcc-x86-64-linux-gnu \
        libssl-dev \
        pkg-config

# https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=733644
RUN cp /usr/include/x86_64-linux-gnu/openssl/opensslconf.h /usr/include/openssl/opensslconf.h

WORKDIR /app

COPY ./src/ ./src
COPY ./Cargo.toml ./Cargo.toml

ENV RUSTFLAGS='-C link-arg=-lpthread -C link-arg=-lm -C target-feature=-crt-static'
ENV PKG_CONFIG_SYSROOT_DIR=/
ENV OPENSSL_LIB_DIR="/usr/lib/x86_64-linux-gnu"
ENV OPENSSL_INCLUDE_DIR="/usr/include"

RUN cargo build --target x86_64-unknown-linux-musl --release --bin dynamo-stream

FROM gcr.io/distroless/cc-debian11:nonroot

WORKDIR /app

COPY --from=builder --chown=nonroot:nonroot /app/target/x86_64-unknown-linux-musl/release/dynamo-stream ./

CMD ["/app/dynamo-stream"]
