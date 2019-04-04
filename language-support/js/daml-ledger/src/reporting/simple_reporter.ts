// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Reporter } from ".";
import * as validation from '../validation';

function spellOutError(error: validation.Error): string {
    switch (error.kind) {
        case 'missing-key':
            return `Missing key ${error.expectedKey} of type ${error.expectedType}`;
        case 'non-unique-union':
            if (error.keys.length === 0) {
                return `Exactly one value must be defined in an union but none are`;
            } else {
                return `Exactly one value must be defined in an union but ${error.keys.length} are (${error.keys.join(', ')})`;
            }
        case 'type-error':
            return `Type error, ${error.expectedType} expected but got ${error.actualType}`;
        case 'unexpected-key':
            return `Unexpected key ${error.key} found`;
    }
}

function buildErrorMessage(tree: validation.Tree, indent: number = 0): string {
    let errors = '';
    for (const error of tree.errors) {
        errors += `\n${' '.repeat(indent)}✗ ${spellOutError(error)}`;
    }
    for (const child in tree.children) {
        if (!validation.ok(tree.children[child])) {
            return `${errors}\n${' '.repeat(indent)}▸ ${child}${buildErrorMessage(tree.children[child], indent + 2)}`;
        }
    }
    return errors;
}

export const SimpleReporter: Reporter = (tree: validation.Tree) => {
    return new Error(`! Validation error${buildErrorMessage(tree)}`);
}