// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Commands } from './commands';

export const SubmitAndWaitRequest: jsc.Arbitrary<ledger.SubmitAndWaitRequest> =
    Commands.smap<ledger.SubmitAndWaitRequest>(
        commands => ({
            commands: commands,
        }),
        request =>
            request.commands
    );