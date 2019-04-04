// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Error, MissingKey, TypeError, UnexpectedKey } from './error';
import { ok, Tree } from "./tree";
import { OptionalKeys, OptionalValidation, RequiredKeys, RequiredSlice, RequiredValidation } from './typelevel';
export { array } from './array';
export { enumeration } from './enumeration';
export { native } from './native';
export { object } from './object';
export { union } from './union';
export { Tree, ok };
export { Error, MissingKey, TypeError, UnexpectedKey };
export { OptionalKeys, RequiredKeys, RequiredSlice };

export interface Validation {
    type: string
    validate(value: any): Tree
    validate(value: any, key: string, validation: Tree): Tree
}

export interface UnionValidation<A extends object> extends Validation {
    values(): Record<keyof A, Validation>
}

export interface ObjectValidation<A extends object> extends Validation {
    required(): RequiredValidation<A>
    optional(): OptionalValidation<A>
}

export const noFields = () => ({});

export function init(key?: string, validation?: Tree): Tree {
    const node: Tree = {
        children: {},
        errors: []
    };
    if (key && validation) {
        validation.children[key] = node;
    }
    return node;
}

export function typeOf(value: any): 'string' | 'number' | 'boolean' | 'symbol' | 'undefined' | 'null' | 'object' | 'function' | 'array' {
    if (value === null) return 'null';
    const t = typeof value;
    return t === 'object' && Array.isArray(value) ? 'array' : t;
}