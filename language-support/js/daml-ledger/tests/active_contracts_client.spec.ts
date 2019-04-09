// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import { MockedActiveContractsServiceClient } from './mock';
import * as grpc from 'daml-grpc';
import * as ledger from '../src';
import { reporting } from '../src';
import * as mapping from '../src/mapping';
import * as validation from '../src/validation';
import * as client from '../src/client';
import * as sinon from 'sinon';

describe("ActiveContractClient", () => {

    const ledgerId = 'some-ledger-id';
    const latestRequestSpy = sinon.spy();

    afterEach(() => {
        sinon.restore();
        latestRequestSpy.resetHistory();
    });

    test('[1.2] Transaction filter and verbose flag are passed to the ledger', (done) => {
        const response = new grpc.GetActiveContractsResponse();
        response.setOffset("10");
        const responses = [response];
        const acs = new MockedActiveContractsServiceClient(responses, latestRequestSpy);
        const acc = new client.ActiveContractsClient(ledgerId, acs, reporting.JSONReporter);
        const request = {
            verbose: false,
            filter: {
                filtersByParty: {
                    alice: { inclusive: { templateIds: [{ packageId: 'packageId', moduleName: 'mod1', entityName: 'ent1' }] } }
                }
            }
        };

        const call = acc.getActiveContracts(request);
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(mapping.GetActiveContractsRequest.toObject(latestRequestSpy.lastCall.lastArg)).to.deep.equal(request);
            done();
        });
        call.on('error', (error) => {
            done(error);
        });

    });

    test('[1.2] The verbose flag defaults to true when not specified', (done) => {
        const response = new grpc.GetActiveContractsResponse();
        response.setOffset("10");
        const responses = [response];
        const acs = new MockedActiveContractsServiceClient(responses, latestRequestSpy);
        const acc = new client.ActiveContractsClient(ledgerId, acs, reporting.JSONReporter);
        const request = {
            filter: {
                filtersByParty: {
                    alice: { inclusive: { templateIds: [{ packageId: 'packageId', moduleName: 'mod1', entityName: 'ent1' }] } }
                }
            }
        };

        const call = acc.getActiveContracts(request);
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetActiveContractsRequest;
            expect(spiedRequest.getVerbose()).to.be.true;
            done();
        });
        call.on('error', (error) => {
            done(error);
        });

    });

    test("[1.3] ACS is requested with the correct ledger ID", (done) => {
        const response = new grpc.GetActiveContractsResponse();
        response.setOffset("10");
        const responses = [response];
        const acs = new MockedActiveContractsServiceClient(responses, latestRequestSpy);
        const acc = new client.ActiveContractsClient(ledgerId, acs, reporting.JSONReporter);
        const request = {
            verbose: true,
            filter: {
                filtersByParty: {
                    alice: { inclusive: { templateIds: [{ packageId: 'packageId', moduleName: 'mod1', entityName: 'ent1' }] } }
                }
            }
        };

        const call = acc.getActiveContracts(request);
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetActiveContractsRequest);
            const request = latestRequestSpy.lastCall.lastArg as grpc.GetActiveContractsRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            done();
        });
        call.on('error', (error) => {
            done(error);
        });

    });

    test('Validation: an ill-formed message does not pass validation (getActiveContracts)', (done) => {

        const response = new grpc.GetActiveContractsResponse();
        response.setOffset("10");
        const responses = [response];
        const acs = new MockedActiveContractsServiceClient(responses, latestRequestSpy);
        const acc = new client.ActiveContractsClient(ledgerId, acs, reporting.JSONReporter);

        const invalidRequest = {
            filter: {
                filtersByarty: {
                    barbara: {
                        inclusive: {}
                    }
                }
            }
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                filter: {
                    errors: [{
                        kind: 'missing-key',
                        expectedKey: 'filtersByParty',
                        expectedType: 'Record<string, Filters>'
                    }, {
                        kind: 'unexpected-key',
                        key: 'filtersByarty'
                    }],
                    children: {}
                }
            }
        };

        let passed = false;
        const call = acc.getActiveContracts(invalidRequest as any as ledger.GetActiveContractsRequest);
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
