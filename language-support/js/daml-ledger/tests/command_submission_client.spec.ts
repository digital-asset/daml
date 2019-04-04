// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import * as sinon from 'sinon';
import { MockedCommandSubmissionClient } from './mock';
import * as ledger from '../src'
import * as grpc from 'daml-grpc'
import { reporting } from '../src';
import * as mapping from '../src/mapping';
import * as validation from '../src/validation';

describe('CommandSubmissionClient', () => {

    const ledgerId = 'watman';
    const latestRequestSpy = sinon.spy();
    const mockedGrpcClient = new MockedCommandSubmissionClient(latestRequestSpy);
    const client = new ledger.CommandSubmissionClient(ledgerId, mockedGrpcClient, reporting.JSONReporter);

    const request: ledger.SubmitRequest = {
        commands: {
            applicationId: '2345',
            commandId: 'sfdgsdfg',
            ledgerEffectiveTime: { seconds: 5445, nanoseconds: 2342 },
            maximumRecordTime: { seconds: 656, nanoseconds: 634 },
            party: '452g245',
            workflowId: 'dfg346',
            list: [
                {
                    create: {
                        templateId: { packageId: 'fgdfg', name: 'dwgwdfg' },
                        arguments: {
                            recordId: { name: '314tgg5', packageId: 'g3g42' },
                            fields: {
                                contract: { contractId: 'sdg4tr34' },
                                someFlag: { bool: true }
                            }
                        }
                    }
                }, {
                    exercise: {
                        choice: 'sdfgv34g',
                        argument: {
                            decimal: '999'
                        },
                        contractId: 'f4f34f34f',
                        templateId: { packageId: 'f1234f34f', name: '341f43f3' }
                    }
                }
            ]
        }
    };

    afterEach(() => {
        sinon.restore();
        latestRequestSpy.resetHistory();
    });

    test('[3.1] When a command is sent to the binding, it is sent to the ledger', (done) => {
        client.submit(request, (error, _) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.SubmitRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.SubmitRequest;
            expect(mapping.SubmitRequest.toObject(spiedRequest)).to.deep.equal(request);
            done();
        });
    });

    test('[3.2] Command is sent to the correct ledger ID', (done) => {
        client.submit(request, (error, _) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.SubmitRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.SubmitRequest;
            expect(spiedRequest.getCommands()!.getLedgerId()).to.equal(ledgerId);
            done();
        });
    });

    test('Validation: an ill-formed message does not pass validation (submit)', (done) => {

        const invalidRequest = {
            commands: {
                applicationId: 'app',
                commandId: 'cmd',
                party: 'birthday',
                ledgerEffectiveTime: { seconds: 0, nanoseconds: 1 },
                maximumRecordTime: { seconds: 1, nanoseconds: 2 },
                list: [
                    {
                        archive: {
                            templateId: {
                                name: 'foo',
                                packageId: 'bar'
                            }
                        }
                    }
                ]
            }
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                commands: {
                    errors: [],
                    children: {
                        applicationId: {
                            errors: [],
                            children: {}
                        },
                        commandId: {
                            errors: [],
                            children: {}
                        },
                        party: {
                            errors: [],
                            children: {}
                        },
                        ledgerEffectiveTime: {
                            errors: [],
                            children: {
                                seconds: {
                                    errors: [],
                                    children: {}
                                },
                                nanoseconds: {
                                    errors: [],
                                    children: {}
                                }}
                        },
                        maximumRecordTime: {
                            errors: [],
                            children: {
                                seconds: {
                                    errors: [],
                                    children: {}
                                },
                                nanoseconds: {
                                    errors: [],
                                    children: {}
                                }}
                        },
                        list: {
                            errors: [],
                            children: {
                                '0': {
                                    errors: [{
                                        kind: 'unexpected-key',
                                        key: 'archive'
                                    }],
                                    children: {}
                                }
                            }
                        }
                    }
                },
            }
        }

        client.submit(invalidRequest as any as ledger.SubmitRequest, error => {
            expect(error).to.not.be.null;
            expect(JSON.parse(error!.message)).to.deep.equal(expectedValidationTree);
            done();
        });

    });

});