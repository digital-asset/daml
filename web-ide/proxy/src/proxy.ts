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
import express, { Response } from "express"
import fs from "fs"
import cookie from "cookie"
import path from "path"
import * as Session from "./session"
import ManagementRoute from "./routes/management"
import WebIdeRoute from "./routes/webide"
import { Server } from "net";
import StaticRoute from "./routes/landing";
import HttpToHttpsRoute from "./routes/httpToHttps";

const docker = new Docker()
const conf = require('./config').read()
const webIdeApp = express()
const managementApp = express()
const httpToHttpsApp = express()
const rootDir = path.dirname(__dirname)
let stopContainersOnShutdown = true

console.log("INFO root dir = %s", rootDir)

if (!conf.http.managementPort) throw new Error("MUST configure management port: 'conf.http.managementPort'")
new ManagementRoute(managementApp).init()
http.createServer(managementApp).listen(conf.http.managementPort, () => {
    console.log("INFO management server listening on port %s", conf.http.managementPort)
})

if (!conf.http.httpToHttpsPort) throw new Error("MUST configure port for insecure redirect: 'conf.http.httpToHttpsPort'")
new HttpToHttpsRoute(httpToHttpsApp).init()
http.createServer(httpToHttpsApp).listen(conf.http.httpToHttpsPort, () => {
    console.log("INFO redirecting all insecure traffic on port %s to https", conf.http.httpToHttpsPort)
})

if (!conf.http.port) throw new Error("MUST configure port for webide: 'conf.http.port'")
const webideServer = createWebIdeServer()
if (conf.secureHeaders || conf.secureHeaders === undefined) {
    webIdeApp.use((req, res, next) => {
        addSecureHeaders(res, conf)
        next()
    })
}
const webIdeRoute = new WebIdeRoute(webIdeApp, webideServer, docker, rootDir).init()
const landingRoute = new StaticRoute(webIdeApp, rootDir).init()
webIdeApp.get('*', (req, res, next) => {
    if (!req.cookies.accepted) return landingRoute.handleLanding(req,res, next)
    else return webIdeRoute.handleHttpRequest(req,res)
})
webIdeApp.use((err :Error, req: express.Request, res :express.Response, next :express.NextFunction) => {
    if (res.headersSent) {
        return next(err)
    }
    webIdeRoute.sendErrorResponse(err, req, res)
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
        console.log("INFO session timed out for containerId=%s", state.docker.Id)
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
            console.log(`INFO docker proxy listening on port ${conf.http.port}!`);
            docker.getImage(conf.docker.webIdeReference)
            .then(image => {
                console.log(`INFO found image ${image.Id} ${image.RepoTags}`);
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
    docker.api.listContainers({all: false, filters: { ancestor: [`${conf.docker.webIdeReference}`] }})
    .then(containers => {
        containers.forEach(c => {
            console.log("INFO removing container %s", c.Id)
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

function addSecureHeaders(res :Response, config :any) {
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubdomains")
    res.setHeader("X-Content-Type-Options", "nosniff")
    res.setHeader("X-Frame-Options", "sameorigin")
    res.setHeader("X-XSS-Protection", "1; mode=block")
    res.setHeader("Referrer-Policy", "no-referrer-when-downgrade")
    res.setHeader("Content-Security-Policy", `default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data:;`)
}
