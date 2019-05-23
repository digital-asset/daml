# This docker file runs the proxy server, which in turn creates docker containers running the web ide. 
# We don't run docker within the containers created by this image, instead we mount docker.sock and use the
# docker binaries to start web ide containers on the host
#
# for example: docker run --rm -it -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock 753e224e9b8b
#
# docker build --rm -t digitalasset/daml-webide-proxy:latest


FROM node:8.16-alpine

RUN mkdir -p /webide-proxy/src
COPY src /webide-proxy/src/
COPY static /webide-proxy/static
COPY *.json /webide-proxy/

WORKDIR /webide-proxy

RUN apk update \
    && apk add docker \
    && npm install \
    && npm run compile

# 3001: management port
# 3002: gcp load balancer will direct http:80 traffic here. See httpToHttps.ts for more details
EXPOSE 80 443 3001 3002
LABEL WEB-IDE-PROXY=""
CMD ["npm", "run", "start"]