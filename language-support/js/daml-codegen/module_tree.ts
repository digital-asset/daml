// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { lf } from 'daml-grpc';

export interface ModuleTree {
    children: Record<string, ModuleTree>,
    modules: lf.v1.Module[]
}

function empty(): ModuleTree {
    return {
        children: {},
        modules: []
    };
}

function insert(module: lf.v1.Module, tree: ModuleTree, depth: number = 0): void {
    const segments = module.getName().getSegmentsList();
    const last = segments.length - 1;
    if (depth < last) {
        const segment = segments[depth];
        if (!tree.children[segment]) {
            tree.children[segment] = empty();
        }
        insert(module, tree.children[segment], depth + 1);
    } else {
        tree.modules.push(module);
    }
}

export function treeify(modules: lf.v1.Module[]): ModuleTree {
    const tree: ModuleTree = empty();
    for (const m of modules) {
        insert(m, tree);
    }
    return tree;
}
