// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect } from 'chai';
import { MockedLedgerIdentityClient } from './mock';
import * as ledger from '../src';

describe("LedgerIdentityClient", () => {

    const ledgerId = 'cafebabe';

    test("[6.1] When the ledger ID is requested, it is returned from the ledger", (done) => {
        const lis = new MockedLedgerIdentityClient(ledgerId);
        const lic = new ledger.LedgerIdentityClient(lis);
        lic.getLedgerIdentity((error, response) => {
            expect(error).to.be.null;
            expect(response!.ledgerId).to.equal(ledgerId);
            done();
        });
    });

});
