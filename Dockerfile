FROM rust:1.86-bookworm@sha256:300ec56abce8cc9448ddea2172747d048ed902a3090e6b57babb2bf19f754081 AS build

WORKDIR /src
COPY . .
RUN cargo build --release -p aether_api --bin aether_pilot_service

FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin aether
COPY --from=build /src/target/release/aether_pilot_service /usr/local/bin/aether_pilot_service

USER aether
WORKDIR /var/lib/aether
ENV AETHER_PILOT_CONFIG=/etc/aether/pilot-service.json
EXPOSE 3000

ENTRYPOINT ["aether_pilot_service"]
CMD ["--config", "/etc/aether/pilot-service.json"]
