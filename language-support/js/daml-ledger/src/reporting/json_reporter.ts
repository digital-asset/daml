// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Reporter } from ".";
import * as validation from '../validation';

export const JSONReporter: Reporter = (tree: validation.Tree) => {
    return new Error(JSON.stringify(tree));
}