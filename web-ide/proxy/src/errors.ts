// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export class ProxyError extends Error {
    status :number
    clientResponse :string
    constructor (message :string, status ?:number, clientResponse ?:string) {
        super(message);
        this.name = this.constructor.name;
        this.status = status || 500;  
        this.clientResponse = clientResponse || "Unknown error occured."
    }
}