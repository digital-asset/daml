// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import { MockedCommandClient } from './mock';
import * as sinon from 'sinon';
import * as ledger from '../src';
import { reporting } from '../src';
import * as mapping from '../src/mapping';
import * as validation from '../src/validation';

describe("CommandClient", () => {

    afterEach(() => {
        sinon.restore();
        lastRequestSpy.resetHistory();
    });

    const emptyRequest: ledger.SubmitAndWaitRequest = {
        commands: {
            applicationId: 'some-application-id',
            commandId: 'some-command-id',
            ledgerEffectiveTime: { seconds: 42, nanoseconds: 47 },
            maximumRecordTime: { seconds: 37, nanoseconds: 999 },
            party: 'birthday-party',
            workflowId: 'some-workflow-id',
            list: []
        }
    };

    const request: ledger.SubmitAndWaitRequest = {
        commands: {
            applicationId: 'some-application-id',
            commandId: 'some-command-id',
            ledgerEffectiveTime: { seconds: 42, nanoseconds: 47 },
            maximumRecordTime: { seconds: 37, nanoseconds: 999 },
            party: 'birthday-party',
            workflowId: 'some-workflow-id',
            list: [
                {
                    create: {
                        templateId: { packageId: 'tmplt', name: 'cpluspls' },
                        arguments: {
                            recordId: { packageId: 'pkg', name: 'fernando' },
                            fields: {
                                someValue: { bool: true }
                            }
                        }
                    }
                }
            ]
        }
    };

    const lastRequestSpy = sinon.spy();
    const mockedClient = new MockedCommandClient(lastRequestSpy);
    const client = new ledger.CommandClient('some-ledger-id', mockedClient, reporting.JSONReporter);

    test('[2.1] When a command is sent to the bindings, it is sent to the ledger', (done) => {
        client.submitAndWait(emptyRequest, (error, _) => {
            expect(error).to.be.null;
            assert(lastRequestSpy.calledOnce);
            expect(lastRequestSpy.lastCall.args).to.have.length(1);
            expect(mapping.SubmitAndWaitRequest.toObject(lastRequestSpy.lastCall.lastArg)).to.deep.equal(emptyRequest);
            done();
        });
    });

    test('[2.2] Command is sent to the correct ledger ID', (done) => {
        client.submitAndWait(request, (error, _) => {
            expect(error).to.be.null;
            assert(lastRequestSpy.calledOnce);
            expect(lastRequestSpy.lastCall.args).to.have.length(1);
            expect(mapping.SubmitAndWaitRequest.toObject(lastRequestSpy.lastCall.lastArg)).to.deep.equal(request);
            done();
        });
    });

    test('Validation: an ill-formed message does not pass validation (submitAndWait)', (done) => {

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

        client.submitAndWait(invalidRequest as any as ledger.SubmitAndWaitRequest, error => {
            expect(error).to.not.be.null;
            expect(JSON.parse(error!.message)).to.deep.equal(expectedValidationTree);
            done();
        });

    });

});
