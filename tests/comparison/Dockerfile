FROM rust:1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests/comparison ./tests/comparison
ENV CARGO_TARGET_DIR=/build-target
RUN cargo test --locked --release --manifest-path tests/comparison/Cargo.toml --lib && \
    cargo test --locked --release --manifest-path tests/comparison/Cargo.toml --no-run && \
    cargo build --locked --release --manifest-path tests/comparison/Cargo.toml --bins && \
    cargo build --locked --release --bin bench-server && \
    mkdir /benchmark-tests && \
    for database in avalanche redis valkey memcached dragonfly keydb; do \
      find /build-target/release/deps -maxdepth 1 -type f -executable -name "$database-*" -exec cp {} /benchmark-tests/$database \; ; \
    done

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /build-target/release/bench-client /usr/local/bin/bench-client
COPY --from=build /build-target/release/bench-server /usr/local/bin/bench-server
COPY --from=build /workspace/tests/comparison/Cargo.lock /opt/benchmark/Cargo.lock
COPY --from=build /benchmark-tests /opt/benchmark-tests
ENTRYPOINT ["bench-client"]
