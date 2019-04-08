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
    Session = require('./session'),
    docker = require('./docker'),
    config = require('./config.json')

class ProxyError extends Error {
    constructor (message, status) {
        super(message);
        this.name = this.constructor.name;
        this.status = status || 500;  
    }
}

const proxy = new httpProxy.createProxyServer({}),
      getImage = docker.getImage(config.docker.image)

const proxyServer = http.createServer((req, res) => handleHttpRequest(req, res));
//start listening once docker is initialized
docker.init()
.then(() => {
    proxyServer.listen(config.http.port, () => {
        console.log(`docker proxy listening on port ${config.http.port}!`)
        getImage
            .then(image => console.log(`found image ${image.Id} ${image.RepoTags}`))
            .catch(e => console.error(e))
    })
})
.catch(err => {
    console.error("error initializing proxy", err)
    process.exit(1)
})

/**
 * Listen to the `upgrade` event and proxy the WebSocket requests.
 */
proxyServer.on('upgrade', (req, socket, head) => handleWsRequest(req, socket, head));

function handleHttpRequest(req, res) {
    try {
        if (config.devMode && req.url === '/session-status') {
            handleSessionStatus(res);
            return
        }
        //console.log("requesting %s", req.url)
        Session.session(req, res, function (err, state, sessionId, saveSession) {
            getImage
                .then(image => ensureDockerContainer(req, state, saveSession, image))
                .then(containerInfo => {
                    const url = docker.getContainerUrl(containerInfo, 'http')
                    //console.log("forwarding to %s", url.href)
                    proxy.web(req, res, { target: url.href })
                })
                .catch(err => {
                    console.error(`could not initiate connection to web-ide: ${err}`)
                    if (err instanceof ProxyError) res.statusCode = err.status
                    else res.statusCode = 500
                    res.end()
                })
        })
    } catch (error) {
        console.error(error)
        res.statusCode = 500
        res.end()
    }
}

function handleWsRequest(req, socket, head) {
    try {
        //console.log('ws connected %s cookie: %O', req.url, req.headers.cookie)
        Session.readSession(req, function (err, state, sessionId) {
            if (!state.docker) {
                return
            }
            //keep session active upon any data
            socket.on('data', () => {
                //console.log("keep active: session[%s] container[%s]", sessionId, state.docker.Id)
                Session.keepActive(sessionId)    //TODO debounce this as it could get chatty   
            })
            const url = docker.getContainerUrl(state.docker, 'ws')
            proxy.ws(req, socket, head, { target: url.href });
        })
    } catch (error) {
        console.error(error)
    }
}

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

function handleSessionStatus(res) {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    Session.allSessionEntries().then(entries => {
        const body = JSON.stringify(entries)
        res.write(JSON.stringify(body));
        res.end();
    });
}

function destroy() {
    console.info('graceful shutdown.')
    proxyServer.close()
    Session.close()
    
    const dockerOde = docker.getOde()
    dockerOde.listContainers({all: false, filters: { label: [`${config.docker.webIdeLabel}`] }})
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
            state.initializing = true;
            saveSession(state);
            const dockerOde = docker.getOde()
            return dockerOde.listContainers({all: false, filters: { label: [`${config.docker.webIdeLabel}`] }})
                .then(containers => { 
                    if (containers.length >= config.docker.maxInstances) {
                        state.initializing = false;
                        saveSession(state);
                        return Promise.reject(new ProxyError(`Breach max instances ${config.docker.maxInstances}`, 503)) 
                    }
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
