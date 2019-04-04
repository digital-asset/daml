// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { init, ObjectValidation, Tree, typeOf, Validation } from ".";
import { OptionalValidation, RequiredValidation } from "./typelevel";

function checkRequired(object: any, required: {[_: string]: Validation}, node: Tree): void {
    for (const key in required) {
        if (object.hasOwnProperty(key)) {
            required[key].validate(object[key], key, node);
        } else {
            node.errors.push({
                kind: 'missing-key',
                expectedKey: key,
                expectedType: required[key].type
            });
        }
    }
}

function checkOptional(object: any, optional: {[_: string]: Validation}, node: Tree): void {
    for (const key in optional) {
        if (object.hasOwnProperty(key) && object[key] !== undefined) {
            optional[key].validate(object[key], key, node);
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

export function object<A extends object>(type: string, required: () => RequiredValidation<A>, optional: () => OptionalValidation<A>): ObjectValidation<A> {
    return {
        type: type,
        required: required,
        optional: optional,
        validate(value: any, key?: string, validation?: Tree): Tree {
            const node = init(key, validation);
            const actualType = typeOf(value);
            if (actualType !== 'object') {
                node.errors.push({
                    kind: 'type-error',
                    expectedType: type,
                    actualType: actualType
                })
            } else {
                const required = this.required();
                const optional = this.optional();
                checkRequired(value, required, node);
                checkOptional(value, optional, node);
                checkUnexpected(value, Object.assign({}, required, optional), node);
            }
            return validation || node;
        }
    }
}