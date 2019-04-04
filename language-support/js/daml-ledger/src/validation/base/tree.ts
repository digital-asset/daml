// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Error } from "./error";

export interface Tree {
    errors: Error[]
    children: Record<string, Tree>
}

export function ok(tree: Tree): boolean {
    return tree.errors.length === 0 && Object.keys(tree.children).every(child => ok(tree.children[child]))
}
