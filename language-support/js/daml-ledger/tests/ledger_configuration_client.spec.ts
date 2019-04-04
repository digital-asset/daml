// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import * as grpc from 'daml-grpc';
import * as ledger from '../src';
import { MockedLedgerConfigurationClient } from './mock';
import * as mapping from '../src/mapping';
import * as sinon from 'sinon';

describe('LedgerConfigurationClient', () => {

    const ledgerId = 'cafebabe';
    const latestRequestSpy = sinon.spy();
    const responses: ledger.GetLedgerConfigurationResponse[] = [
        {
            config: {
                maxTtl: {
                    seconds: 10,
                    nanoseconds: 10
                },
                minTtl: {
                    seconds: 5,
                    nanoseconds: 5
                }
            }
        },
        {
            config: {
                maxTtl: {
                    seconds: 20,
                    nanoseconds: 20
                },
                minTtl: {
                    seconds: 2,
                    nanoseconds: 2
                }
            }
        }
    ]
    const responseMessages = responses.map((response) => mapping.GetLedgerConfigurationResponse.toMessage(response));
    const mockedGrpcClient = new MockedLedgerConfigurationClient(responseMessages, latestRequestSpy);
    const client = new ledger.LedgerConfigurationClient(ledgerId, mockedGrpcClient);

    afterEach(() => {
        sinon.restore();
        latestRequestSpy.resetHistory();
    });

    test('[5.1] When the configuration is requested, then the configuration is returned from the ledger', (done) => {

        const call = client.getLedgerConfiguration();
        let counter = 0;
        call.on('error', (error) => {
            done(error);
        });
        call.on('data', (response) => {
            expect(response).to.deep.equal(responses[counter]);
            counter = counter + 1;
        });
        call.on('end', () => {
            expect(counter).to.equal(responses.length);
            done();
        });

    });

    test('[5.2] The configuration is requested with the correct ledger ID', (done) => {

        const call = client.getLedgerConfiguration();
        call.on('error', (error) => {
            done(error);
        });
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetLedgerConfigurationRequest);
            const request = latestRequestSpy.lastCall.lastArg as grpc.GetLedgerConfigurationRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

});