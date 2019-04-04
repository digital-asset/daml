// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect } from 'chai';
import { MockedTransactionServer } from './mock';
import * as ledger from '../src';
import * as mapping from '../src/mapping';
import { ServerCredentials } from 'grpc';
import { rootCertChain, serverCertChain, serverPrivateKey, clientCertChain, clientPrivateKey } from './certs'

describe("End-to-end SSL support", () => {

    const ledgerId = 'cafebabe';

    const serverCredentials = ServerCredentials.createSsl(rootCertChain, [{ cert_chain: serverCertChain, private_key: serverPrivateKey }], true);

    const transactions: ledger.GetTransactionsResponse[] = [
        {
            transactions: [
                {
                    commandId: 'some-command-id',
                    effectiveAt: { seconds: 47, nanoseconds: 42 },
                    events: [],
                    offset: '42',
                    transactionId: 'some-transaction-id',
                    workflowId: 'some-workflow-id'
                }
            ]
        }
    ]
    const server = new MockedTransactionServer(ledgerId, transactions.map((tx) => mapping.GetTransactionsResponse.toMessage(tx)));
    const port = server.bind('localhost:0', serverCredentials);

    before(() => {
        server.start();
    });

    after(() => {
        server.forceShutdown();
    });

    test('Refuse connections from insecure clients', (done) => {
        ledger.DamlLedgerClient.connect({ host: 'localhost', port: port }, (error, _) => {
            expect(error).to.not.be.null;
            done();
        });
    });

    test('Refuse connections from client providing an incomplete set of certificates', (done) => {
        ledger.DamlLedgerClient.connect({ host: 'localhost', port: port, certChain: clientCertChain, privateKey: clientPrivateKey }, (error, _) => {
            expect(error).to.not.be.null;
            done();
        });
    });

    test('Accept a connection from a secure client, have the correct ledgerId', (done) => {
        ledger.DamlLedgerClient.connect({ host: 'localhost', port: port, rootCerts: rootCertChain, certChain: clientCertChain, privateKey: clientPrivateKey }, (error, client) => {
            expect(error).to.be.null;
            expect(client!.ledgerId).to.equal(ledgerId);
            done();
        });
    });

    test('Accept a connection from a secure client, read data from clients', (done) => {
        ledger.DamlLedgerClient.connect({ host: 'localhost', port: port, rootCerts: rootCertChain, certChain: clientCertChain, privateKey: clientPrivateKey }, (error, client) => {
            expect(error).to.be.null;
            const call = client!.transactionClient.getTransactions({ filter: { filtersByParty: {} }, begin: { absolute: '0' } });
            let counter = 0;
            call.on('error', (error) => {
                done(error);
            });
            call.on('data', (response) => {
                expect(response).to.deep.equal(transactions[counter]);
                counter = counter + 1;
            })
            call.on('end', () => {
                expect(counter).to.equal(transactions.length)
                done();
            });
        });
    });

});

