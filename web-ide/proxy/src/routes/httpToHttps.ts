// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Application } from "express"
const conf = require('../config').read()

/**
 * Due to limitations in our gcp load balancers we implement redirects to https ourselves, on a separate port.
 * The load balancer will direct https traffic to our application by http:80 and direct http traffic to http:3002 
 * so that the application can redirect it to https
 */
export default class HttpToHttpsRoute {
    redirectApp :Application
    constructor(redirectApp :Application) {
        this.redirectApp = redirectApp
    }

    init() : HttpToHttpsRoute {
        this.redirectApp.get('*', (req,res) => {
            if(!req.secure) {
                const r = ['https://', req.hostname, ":", 443, req.url].join('')
                return res.redirect(307, r);
            }
            res.send("OK")
        })
        return this
    }
}