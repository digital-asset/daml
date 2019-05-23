// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Session from "../session"
import Docker from "../docker"
import cookieParser from "cookie-parser"
import { createProxyServer } from "http-proxy"
import { ProxyError } from "../errors"
import { Server, Socket } from "net"
import fs from "fs"
import { Application, Request, Response, NextFunction } from "express"
import { ImageInspectInfo } from "dockerode";
import request, { HttpArchiveRequest } from "request"
import zlib from "zlib"
import { Readable } from "stream"

const got = require('got') //the types had some errors with http types, they also have +2K issues raised in github
const conf = require('../config').read()
const debug = require('debug')('webide:route')

export default class WebIdeRoute {
    app :Application
    server :Server
    docker :Docker
    private proxy :any
    private rootDir :string
    private daWebideCss :Buffer
    constructor(app: Application, webideServer :Server, docker :Docker, rootDir :string) {
        this.app = app
        this.server = webideServer
        this.docker = docker
        this.rootDir = rootDir
        this.daWebideCss = fs.readFileSync(`${this.rootDir}/static/css/webide.main.css`)
    }

    init() : WebIdeRoute {
        this.app.use(cookieParser())
        this.app.get('/ide.main.*.css', (req :Request, res :Response, next :NextFunction) => this.handleIdeCss(req, res, next))
        this.server.on('upgrade', (req :Request, socket :Socket, head :any) => this.handleWsRequest(req, socket, head));
        this.initProxy()
        return this
    }

    private initProxy() {
        this.proxy = createProxyServer({})
        this.proxy.on('error', this.proxyError.bind(this))
        this.proxy.on('proxyRes', this.handleProxyResponse.bind(this));
    }

    /*we request the css and add our own css to the end of it. This involves grabbing the port mapping from our session,
    unzipping the response, appending our css and zipping it back up. */
    handleIdeCss(req :Request, res :Response, next: NextFunction) {
        const route = this
        Session.readSession(req, (error, state, _) => {
            if (error) next(error)
            const cHost = route.docker.getContainerUrl(state.docker, 'http')
            if(typeof cHost.href === "string") {
                const urlString = `${cHost.href}${req.url.substr(1)}`
                debug("css appending for request %s", req.url)
                request.get({ url: urlString, headers: req.headers, gzip: true}, (webIdeError: any, webIdeRes: request.Response, body: any) => {
                    if (webIdeError) return next(webIdeError)
                    const s = new Readable
                    s.push(body)
                    s.push(this.daWebideCss)
                    s.push(null)

                    res.setHeader('Content-Type', 'text/css')
                    res.setHeader('Content-Encoding', 'gzip')
                    res.setHeader('Cache-Control', 'max-age=86400,must-revalidate')
                    s.pipe(zlib.createGzip()).pipe(res)
                })
            }
        })
        
    }

    handleHttpRequest(req :Request, res :Response) {
        //TODO let default error handler do its job
        try {
            const route = this
            debug("requesting %s", req.url)
            Session.session(req, res, (err :any, state :any, sessionId :string, saveSession :any) => {
                route.getDockerImage()
                    .then(image => route.ensureDockerContainer(req, state, saveSession, sessionId, image))
                    .then(containerInfo => {
                        const url = route.docker.getContainerUrl(containerInfo, 'http')
                        route.proxy.web(req, res, { target: url.href })
                    })
                    .catch(err => {
                        console.error(`could not initiate connection to web-ide: ${err}`)
                        route.sendErrorResponse(err, req, res)
                    })
            })
        } catch (error) {
            this.sendErrorResponse(error, req, res)
        }
    }

    sendErrorResponse(err :Error, req: Request, res :Response) {
        if (err instanceof ProxyError) {
            res.status(err.status).send(err.clientResponse)
        }
        else {
            res.statusCode = 500
            res.send("Unknown error occured.")
        }
        res.end()
        //TODO render nice error message 
        //res.render('error', { error: err })
    }

    /* we override response messages so as to ensure we don't expose too much information */
    private handleProxyResponse(proxyRes :Response, req :Request, res :Response) {
        if (proxyRes.statusCode >= 500) {
            const err = new ProxyError(`webide response ${proxyRes.statusCode}: ${proxyRes.statusMessage}`, proxyRes.statusCode, "Server could not process request. Try refreshing the browser")
            this.sendErrorResponse(err, req, res)
        } else if (proxyRes.statusCode >= 400) {
            const err = new ProxyError(`webide response ${proxyRes.statusCode}: ${proxyRes.statusMessage}`, proxyRes.statusCode, "Bad request")
            this.sendErrorResponse(err, req, res)
        }
        //doing nothing we let the proxy handle the response
    }

    private getDockerImage() :Promise<ImageInspectInfo> {
        return this.docker.getImage(conf.docker.webIdeReference)
    }

    private proxyError(proxyErr :any, req :Request, res :Response) {
        Session.session(req, res, (err :any, state :any, sessionId :string, saveSession :any) => {
            console.error("proxy error occurred for session[%s], restarting. Error: %o", sessionId, proxyErr)
            this.proxy.removeAllListeners()
            this.resetState(state, saveSession)
            this.initProxy()
        })
    }

    private handleWsRequest(req :Request, socket :Socket, head :any) {
        try {
            const route = this
            debug('ws connected %s cookie: %O', req.url, req.headers.cookie)
            Session.readSession(req, (sessionErr, state, sessionId) => {
                if (!state.docker) {
                    return
                }
                //keep session active upon any data
                socket.on('data', () => {
                    Session.keepActive(sessionId)    //TODO debounce this as it could get chatty   
                })
                socket.on('error', (err) => {
                    console.error("Socket failure", err)
                })
                socket.on('close', () => {
                    const t = Date.now() - state._started
                    const sessionLength = Math.floor(t/1000)
                    this.trackWebIdeInteraction(sessionId, 'close', 'session-length-seconds', sessionLength)
                })
                const url = route.docker.getContainerUrl(state.docker, 'ws')
                route.proxy.ws(req, socket, head, { target: url.href });
            })
        } catch (error) {
            console.error(error)
        }
    }

    private resetState(state :any, saveSession :Session.SaveSession) {
        state.docker = undefined
        state.initializing = false
        saveSession(state)
    }

    private ensureDockerContainer(req :Request, state :any, saveSession :Session.SaveSession, sessionId :string, image :ImageInspectInfo) {
        if (!state.docker) {
            //double check current state whether it is initializing or not
            const currentState :any = Session.getStateSync(sessionId) || {}
            if (!currentState.initializing) {
                state.initializing = true;
                saveSession(state);
                return this.docker.api.listContainers({all: false, filters: { ancestor: [`${conf.docker.webIdeReference}`] }})
                    .then(containers => { 
                        if (containers.length >= conf.docker.maxInstances) {
                            state.initializing = false;
                            saveSession(state);
                            return Promise.reject(new ProxyError(`Breach max instances ${conf.docker.maxInstances}`, 503, "There is unusually high server load. Please try again in a couple of minutes.")) 
                        }
                        return this.docker.startContainer(image.Id).then(c => {
                            this.trackPageLanding(sessionId)
                            console.log("INFO attaching container %s to session %s", c.Id, sessionId)
                            state.initializing = false
                            state.docker = c
                            saveSession(state);
                            return c
                        });
                    });
            } else {
                console.log("INFO request sent during initialization...waiting for docker to come up")
                return new Promise((resolve, reject) => {
                    const interval = setInterval(() => {
                        Session.readSession(req, (err, state, sessionId) => {
                            if (state.docker && !state.initializing) {
                                clearInterval(interval);
                                resolve(state.docker);
                            }
                        })
                    }, 1000)
                })
            }
        }
        return state.docker
    }

    private trackWebIdeInteraction(sessionId :String, action :String, label :String, value :Number) {   
        const data = {
            // API Version.
            v: '1',
            // Tracking ID / Property ID.
            tid: conf.tracking.gaId,
            // Anonymous Client Identifier. This service is not authenticated so we use sessionId.
            cid: sessionId,
            // Event hit type.
            t: 'event',
            // Document host
            dh: conf.http.hostname,
            // page
            dp: '/webide',
            // title
            dt: 'webide',
            // Event category.
            ec: 'webide',
            ea: action,
            el: label,
            ev: value
          };
        
          return this.trackAnalytics(data)
    }

    //TODO add info from request object
    private trackPageLanding(sessionId :String) {   
        const data = {
            // API Version.
            v: '1',
            // Tracking ID / Property ID.
            tid: conf.tracking.gaId,
            // Anonymous Client Identifier. This service is not authenticated so we use sessionId.
            cid: sessionId,
            // Event hit type.
            t: 'pageview',
            // Document host
            dh: conf.http.hostname,
            // page
            dp: '/webide',
            // title
            dt: 'webide'
          };
        
          return this.trackAnalytics(data)
    }

    private trackAnalytics(data :any) {
        if (conf.tracking.enabled) {
            debug("sending analytics %o", data)
            return got.post('http://www.google-analytics.com/collect', {
                body: data,
                form:true
            })
            .then((res: Response) => {
                if (res.statusCode >= 400) console.error("Could not send metric. Response was %s: %s \n%o", res.statusCode, res.statusMessage, data)
            });
        }
    }
}