// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Session from "../session"
import { Application, Request, Response } from "express"

export default class ManagementRoute {
    managementApp :Application
    constructor(managementApp :Application) {
        this.managementApp = managementApp
    }

    init() : ManagementRoute {
        this.managementApp.get('/session-status', (req,res) => this.handleSessionStatus(req, res))
        return this
    }

    private handleSessionStatus(req :Request, res :Response) {
        Session.allSessionEntries().then(entries => res.status(200).json(JSON.stringify(entries)))
    }
}