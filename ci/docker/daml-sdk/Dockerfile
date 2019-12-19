FROM openjdk:8u212-alpine
RUN apk add --no-cache curl bash
ARG VERSION
RUN addgroup -S daml && adduser -S daml -G daml
USER daml
RUN curl https://get.daml.com | sh -s $VERSION \
    && printf "auto-install: false\nupdate-check: never\n" >> /home/daml/.daml/daml-config.yaml

ENV PATH="/home/daml/.daml/bin:${PATH}"
WORKDIR /home/daml
