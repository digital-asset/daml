// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Web IDE Proxy behavior:
 *   Check config.json (or overloaded WEBIDE_PROXY_CONFIG env variable) on hosting machine for 
 *   configuration details. 
 *   This proxy attempts to manage a set of per user docker instances and 
 *   proxy all incoming requests and websocket connections. It uses an evicting 
 *   cache and shuts down docker instance. Keeps cookie based sessionId
 */

import Docker from "./docker"
import http from "http"
import https from "https"
import express from "express"
import fs from "fs"
import cookie from "cookie"
import path from "path"
import * as Session from "./session"
import ManagementRoute from "./routes/management"
import WebIdeRoute from "./routes/webide"
import { Server } from "net";
import StaticRoute from "./routes/landing";

const docker = new Docker()
const conf = require('./config').read()
const webIdeApp = express()
const managementApp = express()
const rootDir = path.dirname(__dirname).split(path.sep).pop()
let stopContainersOnShutdown = true

new ManagementRoute(managementApp).init()
http.createServer(managementApp).listen(conf.http.managementPort, () => {
    console.log("management server listening on port %s", conf.http.managementPort)
})

const webideServer = createWebIdeServer()
const webIdeRoute = new WebIdeRoute(webIdeApp, webideServer, docker).init()
const landingRoute = new StaticRoute(webIdeApp, <string>rootDir).init()
webIdeApp.get('*', (req, res, next) => {
    if (req.cookies.accepted) return webIdeRoute.handleHttpRequest(req,res)
    else return landingRoute.handleLanding(req,res, next)
})
webIdeApp.use((err :Error, req: express.Request, res :express.Response, next :express.NextFunction) => {
    webIdeRoute.errorHandler(err, req, res, next)
})

docker.init()
.then(() => startProxyServer(webideServer))
.catch(err => {
    console.error("error initializing proxy", err)
    process.exit(1)
})

//handle hard stops
process.on('SIGINT', () => close());
process.on('SIGTERM', () => close());

//stop docker container when session timeouts
Session.onTimeout((state :any) => {
    if (state.docker && state.docker.Id) {
        console.log("session timed out for containerId=%s", state.docker.Id)
        docker.stopContainer(state.docker.Id)
    }
})

function createWebIdeServer() : Server{
    if (conf.certificate && conf.certificate.key && conf.certificate.cert) {
        const certs = {
            key: fs.readFileSync('/Users/bolekwisniewski/certs/server.key'),
            cert: fs.readFileSync('/Users/bolekwisniewski/certs/server.cert')
        }
        return https.createServer(certs, webIdeApp);
    } else {
        return http.createServer(webIdeApp)
    }
}

function startProxyServer(proxyServer :Server) {
    return new Promise((resolve, reject) => {
        proxyServer.listen(conf.http.port, () => {
            console.log(`docker proxy listening on port ${conf.http.port}!`);
            docker.getImage(conf.docker.image)
            .then(image => {
                console.log(`found image ${image.Id} ${image.RepoTags}`);
                resolve(image);
            })
            .catch(e => reject(e));
        });
    });
}

function close() {
    console.info('shutting down')
    webideServer.close()
    Session.close()
    
    if (!stopContainersOnShutdown) return
    stopContainersOnShutdown = false //in case we get multiple kill commands
    docker.api.listContainers({all: false, filters: { label: [`${conf.docker.webIdeLabel}`] }})
    .then(containers => {
        containers.forEach(c => {
            console.log("removing container %s", c.Id)
            docker.api.getContainer(c.Id).remove({force: true})
        })
        if (containers.length == 0) {
            process.exit(0)
        } else {
            //give it some time for the actual docker stops
            const wait = setTimeout(() => {
                clearTimeout(wait);
                process.exit(0)
            }, 10000)
        }
    })
}
