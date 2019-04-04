// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const LedgerOffset: jsc.Arbitrary<ledger.LedgerOffset> =
    jsc.oneof([jsc.elements([ledger.LedgerOffset.Boundary.BEGIN, ledger.LedgerOffset.Boundary.END]), jsc.string]).smap<ledger.LedgerOffset>(
        (value) => {
            if (typeof value === 'string') {
                return {
                    absolute: value
                }
            } else {
                return {
                    boundary: value
                }
            }
        },
        (ledgerOffset) => {
            if (ledgerOffset.absolute !== undefined) {
                return ledgerOffset.absolute;
            } else if (ledgerOffset.boundary !== undefined) {
                return ledgerOffset.boundary;
            } else {
                throw new Error('one of the cases must be defined');
            }
        }
    );