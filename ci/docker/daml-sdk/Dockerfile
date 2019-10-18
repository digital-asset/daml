FROM openjdk:8u212-alpine
RUN apk add curl bash
ARG VERSION
RUN addgroup -S daml && adduser -S daml -G daml
USER daml
RUN curl https://get.daml.com | sh -s $VERSION && rm -rf /tmp/*
ENV PATH="/home/daml/.daml/bin:${PATH}"
