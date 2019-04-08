# DAML WEB IDE proxy
This proxies the docker hosted web ide. It spins up a docker instance for each user and forwards http and websocket connections.
Session state is managed by cookie (webide.connect.sid by default)

### Running
For quick developement cycles we can disable some cumbersome security features; remove `docker.hostConfig.NetworkMode` entry from `config.json` and simply run

```
cd web-ide/proxy
npm install
node proxy.js
```

When running on server, the proxy creates containers on an internal network "web-int". In order for the proxy to communicate with them it must run under docker attached to two networks, "web-int" and "web-ext". The network creation and attachment happen automatically but it simply means that in order to run the proxy locally the same way it runs on server you have to run it via docker image and mount docker.sock.

We haven't uploaded to github yet, so you must first create an image.
```
docker build --rm -t digitalasset/daml-webide-proxy:0.11.19-master web-ide/proxy/
```
then run it
```
docker run --rm -it -p 3000:3000 -v /var/run/docker.sock:/var/run/docker.sock ${IMAGE_ID_OF_PROXY}
```

### Container settings
The proxy uses [dockerode](https://github.com/apocas/dockerode) to manage docker containers, which is a small library over the rest based docker API. Most settings can be configured from `config.json` hostConfig entry. [See API details](https://docs.docker.com/engine/api/v1.37/#operation/ContainerCreate) HostConfig entry
