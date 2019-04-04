// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Transform, TransformOptions } from "stream";
import { Mapping } from "../mapping";

export class MappingTransform<M, O> extends Transform {

    private readonly mapping: Mapping<M, O>

    constructor(mapping: Mapping<M, O>, opts?: TransformOptions) {
        super(Object.assign(opts || {}, { objectMode: true }));
        this.mapping = mapping;
    }

    _transform(message: M, _encoding: string, callback: Function): void {
        try {
            const next = message ? this.mapping.toObject(message) : null;
            this.push(next);
            callback();
        } catch (error) {
            callback(error);
        }
    }

}