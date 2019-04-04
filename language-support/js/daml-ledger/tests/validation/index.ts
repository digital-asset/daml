// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as validation from "../../src/validation";

export function pickFrom<A>(as: A[]): A {
    return as[(Math.floor(Math.random()) * as.length) % as.length];
}

function looselyEqual(left: string[], right: string[]): boolean {
    const l = [...left].sort();
    const r = [...right].sort();
    return l.every((v, i) => v === r[i]);
}

export function containsError(errors: validation.Error[], check: validation.Error): boolean {
    return errors.some(error => {
        switch (error.kind) {
            case 'type-error':
                return check.kind === 'type-error' && check.actualType === error.actualType && check.expectedType === error.expectedType;
            case 'unexpected-key':
                return check.kind === 'unexpected-key' && check.key === error.key;
            case 'non-unique-union':
                return check.kind === 'non-unique-union' && looselyEqual(check.keys, error.keys);
            case 'missing-key':
                return check.kind === 'missing-key' && check.expectedType === error.expectedType && check.expectedKey === error.expectedKey;
        }
    });
}