// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Web IDE Proxy behavior:
 *   Check config.json on hosting machine for configuration details
 *   This proxy attempts to manage a set of per user docker instances and 
 *   proxy all incoming requests and websocket connections. It uses an evicting 
 *   cache (defaulted to 1 hour of inactivity) and shuts down docker instance.
 *   Keeps cookie based sessionId (defaulted to max-age of one day, even if user was active)
 */

const http = require('http'),
    httpProxy = require('http-proxy'),
    { URL } = require('url'),
    Session = require('./session'),
    docker = require('./docker'),
    config = require('./config.json')

const proxy = new httpProxy.createProxyServer({}),
      getImage = docker.getImage(config.docker.image)

const proxyServer = http.createServer((req, res) => {
    //console.log("requesting %s", req.url)
    Session.session(req, res, function (err, state, sessionId, saveSession) {
        getImage
            .then(image => ensureDockerContainer(req, state, saveSession, image))
            .then(containerInfo => {
                const url = getContainerUrl(containerInfo, 'http')
                //console.log("forwarding to %s", url.href)
                proxy.web(req, res, { target: url.href })
            })
            .catch(err => console.error(`could not initiate connection: ${err}`))
    })
});

proxyServer.on('error', (err) => {
    console.error("could not handle request", err)
})

/**
 * Listen to the `upgrade` event and proxy the WebSocket requests.
 */
proxyServer.on('upgrade', (req, socket, head) => {
    //console.log('ws connected %s cookie: %O', req.url, req.headers.cookie)
    Session.readSession(req, function (err, state, sessionId) {
        //keep session active upon any data
        socket.on('data', () => {
            Session.keepActive(sessionId)    //TODO debounce this as it could get chatty   
        })
        if (!state.docker) {
            return
        }
        const url = getContainerUrl(state.docker, 'ws')
        proxy.ws(req, socket, head, { target: url.href });
    })
});

proxyServer.listen(config.http.port, () => {
    console.log(`docker proxy listening on port ${config.http.port}!`)
    getImage
        .then(image => console.log(`found image ${image.Id} ${image.RepoTags}`))
        .catch(e => {
            console.error(`could not find image ${config.docker.image}. Only found:`);
            docker.listImages().then(images => images.forEach(element => {
                console.error(`${element.RepoTags.join(", ")}`)
            }));
        })
})

//stop docker container when session timeouts
Session.onTimeout((state) => {
    if (state.docker && state.docker.Id) {
        console.log("session timed out for containerId=%s", state.docker.Id)
        docker.stopContainer(state.docker.Id)
    }
})

//handle hard stops
process.on('SIGINT', () => destroy());
process.on('SIGTERM', () => destroy());

function destroy() {
    console.info('graceful shutdown.')
    proxyServer.close()
    Session.close()
    
    const dockerOde = docker.getOde()
    dockerOde.listContainers({all: false, filters: { label: ["WEB-IDE"] }})
    .then(containers => {
        containers.forEach(c => {
            console.log("removing container %s", c.Id)
            dockerOde.getContainer(c.Id).remove({force: true})
        })
        //give it some time for the actual docker stops
        const wait = setTimeout(() => {
            clearTimeout(wait);
            process.exit(0)
        }, 10000)
    })
}

function ensureDockerContainer(req, state, saveSession, image) {
    if (!state.docker) {
        if (!state.initializing) {
            const dockerOde = docker.getOde()
            return dockerOde.listContainers({all: false, filters: { label: ["WEB-IDE"] }})
                .then(containers => { 
                    if (containers.length >= config.docker.maxInstances) {
                        return Promise.reject(new Error(`Breach max instances ${config.docker.maxInstances}`)) 
                    }
                    state.initializing = true;
                    saveSession(state);
                    return docker.startContainer(image.Id).then(c => {
                        state.initializing = false
                        state.docker = c
                        saveSession(state);
                        return c
                    });
                });
        } else {
            //this occurs sporadically (perhaps when developer tools is open) sending another request 
            //TODO create better promise handling without timeout
            console.log("request sent during initialization...waiting for docker to come up")
            return new Promise((resolve, reject) => {
                Session.readSession(req, (err, state, sessionId) => {
                    const wait = setTimeout(() => {
                        clearTimeout(wait);
                        resolve(state.docker)
                    }, 10000)
                })
            })
        }
    }
    return state.docker
}

function getContainerUrl(containerInfo, protocol) {
    const containerPort = containerInfo.NetworkSettings.Ports['8443/tcp']
    if (containerPort === undefined) throw "Missing coder-server port[8443] mapping on container"

    const url = new URL(`${protocol}://0.0.0.0:${containerPort[0].HostPort}`)
    return url
}
