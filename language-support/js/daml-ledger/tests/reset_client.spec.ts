// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect, assert } from 'chai';
import * as sinon from 'sinon';
import { MockedResetClient } from './mock';
import * as ledger from '../src';
import * as grpc from 'daml-grpc';

describe("ResetClient", () => {

    const ledgerId = 'cafebabe';

    const latestRequestSpy = sinon.spy();

    const client = new ledger.testing.ResetClient(ledgerId, new MockedResetClient(latestRequestSpy));
    
    it("should pass the correct ledgerId", (done) => {
        client.reset((error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.testing.ResetRequest);
            const request = latestRequestSpy.lastCall.lastArg as grpc.testing.ResetRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            done();
        });
    });

});
