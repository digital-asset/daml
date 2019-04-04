// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Error, Tree, Validation, init, typeOf } from ".";

function valuesOf(enumeration: { [_: number]: string }): string[] {
    return Object.keys(enumeration).filter(k => typeof enumeration[k as any] === "number").map(k => enumeration[k as any]);
}

// NOTE: works only for `enum`s without custom initializers
export function enumeration(enumeration: { [_: number]: string }, type: string): Validation {
    return {
        type: type,
        validate(value: any, key?: string, validation?: Tree): Tree {
            const node = init(key, validation);
            const values = valuesOf(enumeration);
            if (!values.some(v => v === value)) {
                const error: Error = {
                    kind: 'type-error',
                    expectedType: this.type,
                    actualType: typeOf(value)
                };
                node.errors.push(error);
            }
            return validation || node;
        }
    }
};