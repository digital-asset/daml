// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Checkpoint } from './checkpoint';
import { Completion } from './completion';
import { maybe } from './maybe';

export const CompletionStreamResponse: jsc.Arbitrary<ledger.CompletionStreamResponse> =
    jsc.pair(maybe(Checkpoint), maybe(jsc.array(Completion))).smap<ledger.CompletionStreamResponse>(
        ([checkpoint, completions]) => ({
            checkpoint: checkpoint,
            completions: completions
        }),
        (request) =>
            [request.checkpoint, request.completions]
    );