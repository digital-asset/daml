# This docker file runs the proxy server, which in turn creates docker containers running the web ide. 
# We don't run docker within the containers created by this image, instead we mount docker.sock and use the
# docker binaries to start web ide containers on the host
#
# for example: docker run --rm -it -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock 753e224e9b8b
#
# docker build --rm -t digitalasset/daml-webide-proxy:latest


FROM node:8.15-alpine

RUN mkdir -p /webide-proxy/src
COPY src /webide-proxy/src/
COPY *.json /webide-proxy/

WORKDIR /webide-proxy

RUN apk update \
    && apk add docker \
    && npm install \
    && npm run compile

EXPOSE 80 443
LABEL WEB-IDE-PROXY=""
CMD ["npm", "run", "start"]