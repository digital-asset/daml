// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Application, Request, Response, NextFunction } from "express"
import express from "express"
import fs from "fs"
import path from "path"
const debug = require('debug')('webide:static:route')

export default class StaticRoute {
    app :Application
    rootDir :string
    constructor(app :Application, rootDir :string) {
        this.app = app
        this.rootDir = rootDir
    }

    init() : StaticRoute {
        //we do some url manipulation just so we don't bump heads with webide (by default all traffic gets proxied into the webide)
        this.app.use('/_internal/css', express.static(path.join(this.rootDir, '/static/css')))
        this.app.use('/_internal/js', express.static(path.join(this.rootDir, '/static/js')))
        this.app.use('/_internal/images', express.static(path.join(this.rootDir, '/static/images')))
        return this
    }

    handleLanding(req :Request, res :Response, next: NextFunction) {
        const f = this.rootDir + '/static/index.html'
        debug("serving %s", f)
        fs.readFile(f, 'utf8', function(err, contents) {
            if (err) {
                next(err)
                return
            }
            res.send(contents)
        });   
        //TODO ?? render using pug template engine
        // res.render(this.dirname + 'static/index', { name: 'Tobi' }, function(err :Error, html: string) {
        //     if (err) next(err)
        //     res.send(html);
        // });
    }
}