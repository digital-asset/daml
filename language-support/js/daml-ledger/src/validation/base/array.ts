// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Error, Tree, Validation, init, typeOf } from ".";

export function array(items: Validation): Validation {
    return {
        type: `Array<${items.type}>`,
        validate(value: any, key?: string, validation?: Tree): Tree {
            const node = init(key, validation);
            if (Array.isArray(value)) {
                value.forEach((item, index) => {
                    items.validate(item, index.toString(), node);
                });
            } else {
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
}