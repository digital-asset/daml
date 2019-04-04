// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import * as sinon from 'sinon';

import { MockedCommandCompletionClient } from './mock'

import * as ledger from '../src';
import * as grpc from 'daml-grpc';
import { reporting } from '../src';
import * as mapping from '../src/mapping';
import * as validation from '../src/validation';

describe('CommandCompletionClient', () => {

    const ledgerId = 'cafebabe';

    const completion1: ledger.CompletionStreamResponse = {
        checkpoint: {
            offset: {
                absolute: 'some-absolute-offset'
            },
            recordTime: { seconds: 47, nanoseconds: 42 }
        },
        completions: [
            {
                commandId: 'befehl-1',
                status: {
                    code: 404,
                    details: [],
                    message: 'the sound of silence'
                }
            }
        ]
    }

    const completion2: ledger.CompletionStreamResponse = {
        checkpoint: {
            offset: {
                boundary: ledger.LedgerOffset.Boundary.END
            },
            recordTime: { seconds: 99, nanoseconds: 999 }
        },
        completions: [
            {
                commandId: 'befehl-21',
                status: {
                    code: 443,
                    details: [
                        {
                            typeUrl: 'some-type-url',
                            value: 'cafebabe'
                        }
                    ],
                    message: 'chatty chatty'
                }
            },
            {
                commandId: 'befehl-22',
                status: {
                    code: 444,
                    details: [
                        {
                            typeUrl: 'some-other-type-url',
                            value: 'deadbeef'
                        }
                    ],
                    message: 'fully fully'
                }
            }
        ]
    }

    const dummyCompletions = [completion1, completion2];

    const dummyCompletionsMessage: grpc.CompletionStreamResponse[] =
        dummyCompletions.map((object) => mapping.CompletionStreamResponse.toMessage(object));

    const dummyEnd: ledger.CompletionEndResponse = {
        offset: {
            absolute: 'some-absolute-offset'
        }
    }
    const dummyEndMessage = mapping.CompletionEndResponse.toMessage(dummyEnd);

    const lastRequestSpy = sinon.spy();
    const mockedGrpcClient = new MockedCommandCompletionClient(dummyCompletionsMessage, dummyEndMessage, lastRequestSpy);
    const client = new ledger.CommandCompletionClient(ledgerId, mockedGrpcClient, reporting.JSONReporter);

    afterEach(() => {
        sinon.restore();
        lastRequestSpy.resetHistory();
    });

    test('[4.1] When the completion end is requested, it is provided from the ledger', (done) => {

        client.completionEnd((error, response) => {
            expect(error).to.be.null;
            expect(response).to.deep.equal(dummyEnd);
            done();
        });

    });

    test('[4.2] The completion end is requested with the correct ledger ID', (done) => {

        client.completionEnd((error, _response) => {
            expect(error).to.be.null;
            assert(lastRequestSpy.calledOnce);
            expect(lastRequestSpy.lastCall.args).to.have.length(1);
            expect(lastRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.CompletionEndRequest);
            const request = lastRequestSpy.lastCall.lastArg as grpc.CompletionEndRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

    test('[4.4] The completion stream is requested with the correct ledger ID, start offset, application ID and requested parties are provided with the request', (done) => {

        const request = { applicationId: 'foobar', offset: { absolute: '31' }, parties: ['alice', 'bob'] };
        const call = client.completionStream(request);
        call.on('end', () => {
            assert(lastRequestSpy.calledOnce);
            expect(lastRequestSpy.lastCall.args).to.have.length(1);
            expect(lastRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.CompletionStreamRequest);
            const spiedRequest = lastRequestSpy.lastCall.lastArg as grpc.CompletionStreamRequest;
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            expect(mapping.CompletionStreamRequest.toObject(spiedRequest)).to.deep.equal(request);
            done();
        });
        call.on('error', (error) => {
            done(error);
        });

    });

    test('Validation: an ill-formed message does not pass validation (getTransactions)', (done) => {

        const invalidRequest = {
            applicationId: 'app',
            offset: {
                absolute: '1'
            },
            parties: ['birthday', 42]
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                applicationId: {
                    errors: [],
                    children: {}
                },
                offset: {
                    errors: [],
                    children: {
                        absolute: {
                            errors: [],
                            children: {}
                        }
                    }
                },
                parties: {
                    errors: [],
                    children: {
                        '0': {
                            errors: [],
                            children: {}
                        },
                        '1': {
                            errors: [{
                                kind: 'type-error',
                                expectedType: 'string',
                                actualType: 'number'
                            }],
                            children: {}
                        }
                    }
                }
            }
        }

        let passed = false;
        const call = client.completionStream(invalidRequest as any as ledger.CompletionStreamRequest);
        call.on('data', (_data) => {
            done(new Error('unexpected data received'));
        });
        call.on('error', (error) => {
            expect(JSON.parse(error.message)).to.deep.equal(expectedValidationTree);
            passed = true;
        });
        call.on('end', () => {
            assert(passed);
            done();
        });

    });

});