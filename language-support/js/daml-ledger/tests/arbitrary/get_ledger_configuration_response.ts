// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { LedgerConfiguration } from './ledger_configuration';

export const GetLedgerConfigurationResponse: jsc.Arbitrary<ledger.GetLedgerConfigurationResponse> =
    LedgerConfiguration.smap<ledger.GetLedgerConfigurationResponse>(
        config => ({
            config: config,
        }),
        request =>
            request.config
    );