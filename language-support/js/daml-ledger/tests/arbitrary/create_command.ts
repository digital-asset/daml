// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Record } from './record_value_variant';
import { Identifier } from './identifier';

export const CreateCommand: jsc.Arbitrary<ledger.CreateCommand> =
    jsc.pair(Record, Identifier).smap<ledger.CreateCommand>(
        ([args, templateId]) => ({
            arguments: args,
            templateId: templateId
        }),
        (createCommand) =>
            [createCommand.arguments, createCommand.templateId]
    );