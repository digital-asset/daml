// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Error, Tree, Validation, init, typeOf } from ".";

export function native(type: 'number' | 'string' | 'boolean'): Validation {
    return {
        type: type,
        validate(value: any, key?: string, validation?: Tree): Tree {
            const node = init(key, validation);
            const actualType = typeOf(value);
            if (actualType !== type) {
                const error: Error = {
                    kind: 'type-error',
                    expectedType: type,
                    actualType: actualType
                };
                node.errors.push(error);
            }
            return validation || node;
        }
    }
}