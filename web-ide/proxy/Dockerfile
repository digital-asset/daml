# This docker file runs the proxy server, which in turn creates docker containers running the web ide. 
# We don't run docker within the containers created by this image, instead we mount docker.sock and use the
# docker binaries to start web ide containers on the host
#
# for example: docker run --rm -it -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock 753e224e9b8b
#
# docker build --rm -t digitalasset/daml-webide-proxy:0.11.19-master


FROM node:8.15-alpine

COPY *.js /
COPY *.json /

RUN npm install \
    && apk update \
    && apk add docker

EXPOSE 3000 8443
LABEL WEB-IDE-PROXY=""
CMD ["node", "proxy.js"]