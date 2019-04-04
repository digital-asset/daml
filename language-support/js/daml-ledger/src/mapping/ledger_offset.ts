// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

import { inspect } from 'util';

export const LedgerOffset: mapping.Mapping<grpc.LedgerOffset, ledger.LedgerOffset> = {
    toObject(ledgerOffset: grpc.LedgerOffset): ledger.LedgerOffset {
        if (ledgerOffset.hasAbsolute()) {
            return { absolute: ledgerOffset.getAbsolute() }
        } else if (ledgerOffset.hasBoundary()) {
            const boundary = ledgerOffset.getBoundary();
            switch (boundary) {
                case grpc.LedgerOffset.LedgerBoundary.LEDGER_BEGIN: {
                    return { boundary: ledger.LedgerOffset.Boundary.BEGIN }
                }
                case grpc.LedgerOffset.LedgerBoundary.LEDGER_END: {
                    return { boundary: ledger.LedgerOffset.Boundary.END }
                }
                default: {
                    throw new Error(`Expected LedgerOffset Boundary, found ${inspect(boundary)}`);
                }
            }
        } else {
            throw new Error(`Expected either LedgerOffset Absolute or LedgerOffset Boundary, found ${inspect(ledgerOffset)}`);
        }
    },
    toMessage(ledgerOffset: ledger.LedgerOffset): grpc.LedgerOffset {
        const result = new grpc.LedgerOffset();
        if (ledgerOffset.boundary !== undefined) {
            switch (ledgerOffset.boundary) {
                case ledger.LedgerOffset.Boundary.BEGIN: {
                    result.setBoundary(grpc.LedgerOffset.LedgerBoundary.LEDGER_BEGIN);
                    break;
                }
                case ledger.LedgerOffset.Boundary.END: {
                    result.setBoundary(grpc.LedgerOffset.LedgerBoundary.LEDGER_END);
                    break;
                }
                default: {
                    throw new Error(`Expected boundary, found ${inspect(ledgerOffset.boundary!)}`);
                }
            }
        } else if (ledgerOffset.absolute) {
            result.setAbsolute(ledgerOffset.absolute);
        } else {
            throw new Error(`Expected either LedgerOffset Absolute or LedgerOffset Boundary, found ${inspect(ledgerOffset)}`);
        }
        return result;
    }
}