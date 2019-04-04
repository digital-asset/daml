// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { assert, expect } from 'chai';
import * as sinon from 'sinon';
import * as grpc from 'daml-grpc';
import * as ledger from '../src';
import { MockedTransactionClient } from './mock';
import { reporting } from '../src';
import * as mapping from '../src/mapping';
import * as validation from '../src/validation';

describe('TransactionClient', () => {

    const transactionsRequest: ledger.GetTransactionsRequest = {
        begin: {
            absolute: '42'
        },
        filter: {
            filtersByParty: {
                someParty: {
                    inclusive: {
                        templateIds: [
                            { name: 'foobar', packageId: 'foo' },
                            { name: 'fooquux', packageId: 'quux' }
                        ]
                    }
                },
                someOtherParty: {}
            }
        },
        verbose: false
    };

    const transactionByIdRequest: ledger.GetTransactionByIdRequest = {
        requestingParties: [],
        transactionId: 'cafebabe'
    };

    const transactionByEventIdRequest: ledger.GetTransactionByEventIdRequest = {
        requestingParties: [],
        eventId: 'some-created-id'
    };

    const latestRequestSpy = sinon.spy();
    const ledgerId = 'deadbeef';
    const mockedGrpcClient = new MockedTransactionClient(ledgerId, latestRequestSpy);
    const client = new ledger.TransactionClient(ledgerId, mockedGrpcClient, reporting.JSONReporter);

    afterEach(() => {
        latestRequestSpy.resetHistory();
        sinon.restore();
    });

    test.skip('[8.1] When transactions are requested, transactions are returned from the ledger', (done) => {

        done(new Error('UNTESTED'));

    });

    test('[8.2] Start offset, end offset, transaction filter and verbose flag are passed with the transactions request to the ledger', (done) => {

        const call = client.getTransactions(transactionsRequest);
        call.on('error', (error) => {
            done(error);
        });
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.instanceof(grpc.GetTransactionsRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionsRequest;
            expect(mapping.GetTransactionsRequest.toObject(spiedRequest)).to.deep.equal(transactionsRequest);
            done();
        });

    });

    test('[8.2] The verbose flag defaults to true when not specified', (done) => {

        const requestWithoutExplicitVerbose = { ...transactionsRequest };
        delete requestWithoutExplicitVerbose.verbose;

        const call = client.getTransactions(requestWithoutExplicitVerbose);
        call.on('error', (error) => {
            done(error);
        });
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.instanceof(grpc.GetTransactionsRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionsRequest;
            expect(spiedRequest.getVerbose()).to.be.true;
            done();
        });

    });

    test('[8.3] Transaction stream is requested with the correct ledger ID', (done) => {

        const call = client.getTransactions(transactionsRequest);
        call.on('error', (error) => {
            done(error);
        });
        call.on('end', () => {
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.instanceof(grpc.GetTransactionsRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionsRequest;
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

    it.skip('[8.4] Transaction stream handles backpressure', (done) => {

        done(new Error('UNTESTED'));

    });

    it.skip('[8.5] When transaction trees are requested, transaction trees are returned from the ledger', (done) => {

        done(new Error('UNTESTED'));

    });

    it.skip('[8.6] Start offset, end offset transaction filter and verbose flag are passed with the transaction trees request to the ledger', (done) => {

        done(new Error('UNTESTED'));

    });

    it.skip('[8.7] Transaction tree stream is requested with the correct ledger ID', (done) => {

        done(new Error('UNTESTED'));

    });

    it.skip('[8.8] Transaction tree stream handles backpressure', (done) => {

        done(new Error('UNTESTED'));

    });

    test.skip('[8.9] Transactions can be looked up by a contained event ID', (done) => {

        done(new Error('UNTESTED'));

    });

    test('[8.10] The requesting parties are passed with the transaction lookup by event ID', (done) => {

        client.getTransactionByEventId(transactionByEventIdRequest, (error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetTransactionByEventIdRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionByEventIdRequest;
            expect(mapping.GetTransactionByEventIdRequest.toObject(spiedRequest)).to.deep.equal(transactionByEventIdRequest);
            done();
        });

    });

    test('[8.11] The transaction lookup by event ID happens with the correct ledger ID', (done) => {

        client.getTransactionByEventId(transactionByEventIdRequest, (error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetTransactionByEventIdRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionByEventIdRequest;
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

    test.skip('[8.12] Transactions can be looked up by transaction ID', (done) => {

        done(new Error('UNTESTED'));

    });

    test('[8.13] The requesting parties are passed with the transaction lookup by transaction ID', (done) => {

        client.getTransactionById(transactionByIdRequest, (error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetTransactionByIdRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionByIdRequest;
            expect(mapping.GetTransactionByIdRequest.toObject(spiedRequest)).to.deep.equal(transactionByIdRequest);
            done();
        });
    });

    test('[8.14] The transaction lookup by transaction ID happens with the correct ledger ID', (done) => {

        client.getTransactionById(transactionByIdRequest, (error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetTransactionByIdRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetTransactionByIdRequest;
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

    test.skip('[8.15] When the ledger end is requested, it is provided from the ledger (non-empty ledger)', (done) => {

        done(new Error('UNTESTED'));

    });

    test.skip('[8.15] When the ledger end is requested, it is provided from the ledger (empty ledger)', (done) => {

        done(new Error('UNTESTED'));

    });

    test('[8.16] The ledger end is requested with the correct ledger ID', (done) => {

        client.getLedgerEnd((error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce);
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetLedgerEndRequest);
            const spiedRequest = latestRequestSpy.lastCall.lastArg as grpc.GetLedgerEndRequest;
            expect(spiedRequest.getLedgerId()).to.equal(ledgerId);
            done();
        });

    });

    test('Validation: an ill-formed message does not pass validation (getTransactionByEventId)', (done) => {

        const invalidRequest = {
            eventId: 'some-event-id',
            requestingParties: 42
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                eventId: {
                    errors: [],
                    children: {}
                },
                requestingParties: {
                    errors: [{
                        kind: 'type-error',
                        expectedType: 'Array<string>',
                        actualType: 'number'
                    }],
                    children: {}
                }
            }
        }

        client.getTransactionByEventId(invalidRequest as any as ledger.GetTransactionByEventIdRequest, error => {
            expect(error).to.not.be.null;
            expect(JSON.parse(error!.message)).to.deep.equal(expectedValidationTree);
            done();
        });

    });

    test('Validation: an ill-formed message does not pass validation (getTransactions)', (done) => {

        const invalidTransactionsRequest: ledger.GetTransactionsRequest = {
            begin: {
                absolute: '42',
                boundary: ledger.LedgerOffset.Boundary.BEGIN
            },
            filter: {
                filtersByParty: {}
            }
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                begin: {
                    errors: [{
                        kind: 'non-unique-union',
                        keys: ['absolute', 'boundary']
                    }],
                    children: {
                        absolute: {
                            errors: [],
                            children: {}
                        },
                        boundary: {
                            errors: [],
                            children: {}
                        }
                    }
                },
                filter: {
                    errors: [],
                    children: {
                        filtersByParty: {
                            errors: [],
                            children: {}
                        }
                    }
                }
            }
        }

        let passed = false;
        const call = client.getTransactions(invalidTransactionsRequest);
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

    test('Validation: an ill-formed message does not pass validation (getTransactionTrees)', (done) => {

        const invalidTransactionsRequest: ledger.GetTransactionsRequest = {
            begin: {
                absolute: '42',
                boundary: ledger.LedgerOffset.Boundary.BEGIN
            },
            filter: {
                filtersByParty: {}
            }
        };

        const expectedValidationTree: validation.Tree = {
            errors: [],
            children: {
                begin: {
                    errors: [{
                        kind: 'non-unique-union',
                        keys: ['absolute', 'boundary']
                    }],
                    children: {
                        absolute: {
                            errors: [],
                            children: {}
                        },
                        boundary: {
                            errors: [],
                            children: {}
                        }
                    }
                },
                filter: {
                    errors: [],
                    children: {
                        filtersByParty: {
                            errors: [],
                            children: {}
                        }
                    }
                }
            }
        }

        let passed = false;
        const call = client.getTransactionTrees(invalidTransactionsRequest);
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
