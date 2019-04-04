// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { init, Tree, Validation, typeOf, UnionValidation } from ".";

function checkUniqueness(object: any, node: Tree): void {
    const defined: string[] = Object.keys(object).filter(key => object[key] !== undefined);
    if (defined.length !== 1) {
        node.errors.push({
            kind: 'non-unique-union',
            keys: defined
        })
    }
}

function checkValue(object: any, values: {[_: string]: Validation}, node: Tree): void {
    for (const key in values) {
        if (object.hasOwnProperty(key)) {
            values[key].validate(object[key], key, node);
        }
    }
}

function checkUnexpected<A extends object>(object: any, keys: Record<keyof A, Validation>, node: Tree): void {
    for (const key in object) {
        if (!keys || !keys.hasOwnProperty(key)) {
            node.errors.push({
                kind: 'unexpected-key',
                key: key
            })
        }
    }
}

export function union<A extends object>(type: string, values: () => Record<keyof A, Validation>): UnionValidation<A> {
    return {
        type: type,
        values: values,
        validate(value: any, key?: string, validation?: Tree): Tree {
            const node = init(key, validation);
            const actualType = typeOf(value);
            if (actualType !== 'object') {
                node.errors.push({
                    kind: 'type-error',
                    expectedType: type,
                    actualType: actualType
                });
            } else {
                const values = this.values();
                checkUniqueness(value, node);
                checkValue(value, values, node);
                checkUnexpected(value, values, node);
            }
            return validation || node;
        }
    };
}