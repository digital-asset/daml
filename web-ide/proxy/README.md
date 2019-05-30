# DAML WEB IDE proxy
This proxies the docker hosted web ide. It spins up a docker instance for each user and forwards http and websocket connections.
Session state is managed by cookie (webide.connect.sid by default)

### Building images
```
docker build --rm -t digitalasset/daml-webide-proxy:latest web-ide/proxy/
docker build --rm -t digitalasset/daml-webide:latest web-ide/ide-server/
```

### Downloading images
Images are stored in a private DA repo `digitalasset/daml-webide-proxy` and `digitalasset/daml-webide`. If you don't have access you can build the image, as mentioned above.
download the proxy and webide images from dockerhub
```
docker pull digitalasset/daml-webide-proxy:latest
docker pull digitalasset/daml-webide:latest
```

### Running
For quick developement cycles we can disable some cumbersome security features; create a copy of `src/config.json` as `src/config.local.json` and remove `docker.hostConfig.NetworkMode` entry. If you have node and npm installed, the below command will run the proxy directly on your host instead of running from within a 
`daml-webide-proxy` container.

```
cd web-ide/proxy
npm install
npm run compile && npm run local-start
```

When running on server, the proxy creates containers on an internal network "web-int". In order for the proxy to communicate with them it must run under docker attached to two networks, "web-int" and "web-ext". The network creation and attachment happen automatically but it simply means that in order to run the proxy locally the same way it runs on server you have to run it via docker image and mount docker.sock.

```
docker run --rm -it -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock ${IMAGE_ID_OF_PROXY}
```

with custom config `~/daml/web-ide/proxy/src/config.local.json`
```
docker run --rm -it -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock -e WEBIDE_PROXY_CONFIG=/webide-proxy/webide.config.json -v ~/daml/web-ide/proxy/src/config.local.json:/webide-proxy/webide.config.json:ro ${IMAGE_ID_OF_PROXY}
```

### Container settings
The proxy uses [dockerode](https://github.com/apocas/dockerode) to manage docker containers, which is a small library over the rest based docker API. Most settings can be configured from `config.json` hostConfig entry. [See API details](https://docs.docker.com/engine/api/v1.37/#operation/ContainerCreate) HostConfig entry
