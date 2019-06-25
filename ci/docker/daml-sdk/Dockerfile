FROM openjdk:8u212-alpine
RUN apk add curl bash
ARG VERSION
RUN curl https://get.daml.com | sh -s $VERSION
ENV PATH="~/.daml/bin:${PATH}"
