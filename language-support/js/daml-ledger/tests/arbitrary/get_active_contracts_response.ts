// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { CreatedEvent } from './event';
import { LedgerOffset } from './ledger_offset';
import { maybe } from './maybe';

export const GetActiveContractsResponse: jsc.Arbitrary<ledger.GetActiveContractsResponse> =
    jsc.tuple([LedgerOffset, maybe(jsc.string), maybe(jsc.array(CreatedEvent))]).smap(
        ([offset, workflowId, activeContracts]) => {
            const request: ledger.GetActiveContractsResponse = {
                offset: offset
            }
            if (workflowId) {
                request.workflowId = workflowId;
            }
            if (activeContracts) {
                request.activeContracts = activeContracts;
            }
            return request;
        },
        (request) => {
            return [request.offset, request.workflowId, request.activeContracts];
        }
    );