# Apache Jena Fuseki, the lakehouse's SPARQL endpoint, built from Apache's own release rather than a third-party
# image. The release's SHA-512 is pinned here (checked against Apache's .sha512 when it was pinned), and every build
# also checks its signature against Apache's Jena KEYS. Config and start script come from the mounted folder.
ARG JRE_TAG=21.0.12_8-jre-noble
FROM eclipse-temurin:${JRE_TAG} AS fetch
ARG FUSEKI_VERSION=6.2.0
ARG FUSEKI_SHA512=ba65f5867d2d4741b2ed9e2af5a0d4fbb447909894ab2a0c6bc4dac8997f4fe339c87b13c48d45d054977769f0f8bf763ea346b1f7792d5cdc458041bd43a132
RUN apt-get update && apt-get install -y --no-install-recommends gnupg && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp/fuseki
RUN set -eu; tgz="apache-jena-fuseki-${FUSEKI_VERSION}.tar.gz"; \
    base="https://archive.apache.org/dist/jena/binaries"; \
    curl -fsSLO "$base/$tgz" && curl -fsSLO "$base/$tgz.asc" && curl -fsSL https://downloads.apache.org/jena/KEYS -o KEYS; \
    echo "${FUSEKI_SHA512}  $tgz" | sha512sum -c -; \
    export GNUPGHOME="$(mktemp -d)"; gpg --batch --quiet --import KEYS; gpg --batch --verify "$tgz.asc" "$tgz"; \
    tar xzf "$tgz" --strip-components=1 "apache-jena-fuseki-${FUSEKI_VERSION}/fuseki-server.jar"

FROM eclipse-temurin:${JRE_TAG}
COPY --from=fetch /tmp/fuseki/fuseki-server.jar /opt/fuseki/fuseki-server.jar
USER 1000:100
EXPOSE 3030
CMD ["bash", "/opt/fuseki/conf/start.sh"]
