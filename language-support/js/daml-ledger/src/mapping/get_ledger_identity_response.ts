// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetLedgerIdentityResponse: mapping.Mapping<grpc.GetLedgerIdentityResponse, ledger.GetLedgerIdentityResponse> = {
    toObject(response: grpc.GetLedgerIdentityResponse): ledger.GetLedgerIdentityResponse {
        return {
            ledgerId: response.getLedgerId()
        }
    },
    toMessage(response: ledger.GetLedgerIdentityResponse): grpc.GetLedgerIdentityResponse {
        const result = new grpc.GetLedgerIdentityResponse();
        result.setLedgerId(response.ledgerId);
        return result;
    }
}