// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Session from "../session"
import Docker from "../docker"
import { createProxyServer } from "http-proxy"
import { ProxyError } from "../errors"
import { Server, Socket } from "net"
import { Application, Request, Response } from "express"
import { ImageInspectInfo } from "dockerode";

const conf = require('../config').read()
const debug = require('debug')('webide-route')

export default class WebIdeRoute {
    app :Application
    server :Server
    docker :Docker
    private proxy :any
    constructor(app: Application, webideServer :Server, docker :Docker) {
        this.app = app
        this.server = webideServer
        this.docker = docker
        this.proxy = createProxyServer({})
    }

    init() : WebIdeRoute {
        this.app.get('*', (req, res) => this.handleHttpRequest(req, res))
        this.server.on('upgrade', (req :Request, socket :Socket, head :any) => this.handleWsRequest(req, socket, head));
        return this
    }

    private getImage() :Promise<ImageInspectInfo> {
        return this.docker.getImage(conf.docker.image)
    }

    private handleHttpRequest(req :Request, res :Response) {
        try {
            const route = this
            debug("requesting %s", req.url)
            Session.session(req, res, (err :any, state :any, sessionId :string, saveSession :any) => {
                route.getImage()
                    .then(image => route.ensureDockerContainer(req, state, saveSession, image))
                    .then(containerInfo => {
                        const url = route.docker.getContainerUrl(containerInfo, 'http')
                        debug("forwarding to %s", url.href)
                        route.proxy.web(req, res, { target: url.href })
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

    private handleWsRequest(req :Request, socket :Socket, head :any) {
        try {
            const route = this
            debug('ws connected %s cookie: %O', req.url, req.headers.cookie)
            Session.readSession(req, (err, state, sessionId) => {
                if (!state.docker) {
                    return
                }
                //keep session active upon any data
                socket.on('data', () => {
                    Session.keepActive(sessionId)    //TODO debounce this as it could get chatty   
                })
                const url = route.docker.getContainerUrl(state.docker, 'ws')
                route.proxy.ws(req, socket, head, { target: url.href });
            })
        } catch (error) {
            console.error(error)
        }
    }

    private ensureDockerContainer(req :Request, state :any, saveSession :Session.SaveSession, image :ImageInspectInfo) {
        if (!state.docker) {
            if (!state.initializing) {
                state.initializing = true;
                saveSession(state);
                return this.docker.api.listContainers({all: false, filters: { label: [`${conf.docker.webIdeLabel}`] }})
                    .then(containers => { 
                        if (containers.length >= conf.docker.maxInstances) {
                            state.initializing = false;
                            saveSession(state);
                            return Promise.reject(new ProxyError(`Breach max instances ${conf.docker.maxInstances}`, 503)) 
                        }
                        return this.docker.startContainer(image.Id).then(c => {
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
}