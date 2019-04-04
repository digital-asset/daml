// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import * as sinon from 'sinon';
import { MockedTimeClient } from './mock';
import * as grpc from 'daml-grpc';
import * as ledger from '../src';
import { reporting } from '../src';
import * as mapping from '../src/mapping';
import * as validation from '../src/validation';

describe('TimeClient', () => {

    const ledgerId = 'cafebabe';
    const latestRequestSpy = sinon.spy();
    const responses: ledger.GetTimeResponse[] = [
        { currentTime: { seconds: 10, nanoseconds: 1 } },
        { currentTime: { seconds: 11, nanoseconds: 1 } },
        { currentTime: { seconds: 12, nanoseconds: 1 } },
        { currentTime: { seconds: 13, nanoseconds: 1 } }
    ];
    const responseMessages = responses.map(response => mapping.GetTimeResponse.toMessage(response));
    const mockedGrpcClient = new MockedTimeClient(responseMessages, latestRequestSpy);
    const client = new ledger.testing.TimeClient(ledgerId, mockedGrpcClient, reporting.JSONReporter);

    const setTimeRequest: ledger.SetTimeRequest = {
        currentTime: { seconds: 4, nanoseconds: 1 },
        newTime: { seconds: 5, nanoseconds: 1 }
    }
    const invalidSetTimeRequest: ledger.SetTimeRequest = {
        currentTime: { seconds: 4, nanoseconds: 1 },
        newTime: { seconds: 3, nanoseconds: 1 }
    }

    afterEach(() => {
        sinon.restore();
        latestRequestSpy.resetHistory();
    });

    test('[9.1] The correct ledger ID, current and new time of the set time request are passed to the ledger', (done) => {

        client.setTime(setTimeRequest, (error, _) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.testing.SetTimeRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.testing.SetTimeRequest;
            expect(mapping.SetTimeRequest.toObject(spiedRequest)).to.deep.equal(setTimeRequest);
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

    test('[9.2] When ledger times are requested, ledger times are returned from the ledger', (done) => {

        const call = client.getTime();

        let counter = 0;
        call.on('data', (response) => {
            expect(response).to.deep.equal(responses[counter]);
            counter = counter + 1;
        });
        call.on('end', () => {
            expect(counter).to.equal(responses.length);
            done();
        });
        call.on('error', (error) => {
            done(error);
        });

    });

    test('[9.3] Ledger times stream is requested with the correct ledger ID', (done) => {

        const call = client.getTime();
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.instanceof(grpc.testing.GetTimeRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.testing.GetTimeRequest;
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            done();
        });
        call.on('error', (error) => {
            done(error);
        });

    });

    // The client in the JS bindings performs no check about the semantics of the message
    // This check is fully delegated to the server, who already implements this logic
    test.skip('[9.4] When ledger time is set, an error is returned if current time >= new time', (done) => {

        client.setTime(invalidSetTimeRequest, (error, _) => {
            expect(error).to.not.be.null;
            done();
        });

    });

    test('Validation: an ill-formed message does not pass validation (setTime)', (done) => {

        const invalidRequest = {
            currentTime: { seconds: 3, nanoseconds: '0' },
            newTime: { seconds: 4, nanoseconds: 0 }
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                currentTime: {
                    errors: [],
                    children: {
                        seconds: {
                            errors: [],
                            children: {}
                        },
                        nanoseconds: {
                            errors: [{
                                kind: 'type-error',
                                expectedType: 'number',
                                actualType: 'string'
                            }],
                            children: {}
                        }
                    }
                },
                newTime: {
                    errors: [],
                    children: {
                        seconds: {
                            errors: [],
                            children: {}
                        },
                        nanoseconds: {
                            errors: [],
                            children: {}
                        }
                    }
                }
            }
        }

        client.setTime(invalidRequest as any as ledger.SetTimeRequest, error => {
            expect(error).to.not.be.null;
            expect(JSON.parse(error!.message)).to.deep.equal(expectedValidationTree);
            done();
        });

    });

});