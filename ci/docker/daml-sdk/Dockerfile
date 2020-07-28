FROM openjdk:8u212-alpine
RUN apk add --no-cache curl bash
ARG VERSION
# This is needed to get the DNS requests
# from Haskell binaries to succeed.
# Otherwise they fail to even resolve localhost.
RUN echo 'hosts: files dns' > /etc/nsswitch.conf
RUN addgroup -S daml && adduser -S daml -G daml
USER daml
RUN curl https://get.daml.com | sh -s $VERSION \
    && printf "auto-install: false\nupdate-check: never\n" >> /home/daml/.daml/daml-config.yaml

ENV PATH="/home/daml/.daml/bin:${PATH}"
WORKDIR /home/daml
