// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as validation from "../validation";

export type Reporter = (_: validation.Tree) => Error

export { SimpleReporter } from './simple_reporter';
export { JSONReporter } from './json_reporter';