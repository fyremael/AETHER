FROM rust:1.78-bookworm AS build

WORKDIR /src
COPY . .
RUN cargo build --release -p aether_api --bin aether_pilot_service

FROM debian:bookworm-slim

RUN useradd --create-home --shell /usr/sbin/nologin aether
COPY --from=build /src/target/release/aether_pilot_service /usr/local/bin/aether_pilot_service

USER aether
WORKDIR /var/lib/aether
ENV AETHER_PILOT_CONFIG=/etc/aether/pilot-service.json
EXPOSE 3000

ENTRYPOINT ["aether_pilot_service"]
CMD ["--config", "/etc/aether/pilot-service.json"]
