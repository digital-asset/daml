// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { MockedTransactionServer } from './mock';
import { ServerCredentials } from 'grpc';
import * as mapping from '../src/mapping';
import * as ledger from '../src';
import { expect } from 'chai';

describe('End-to-end stream', () => {

    const ledgerId = 'cafebabe';

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
    const port = server.bind('0.0.0.0:0', ServerCredentials.createInsecure());

    before(() => {
        server.start();
    });

    after(() => {
        server.forceShutdown();
    });

    it('should correctly move one transaction end-to-end', (done) => {
        ledger.DamlLedgerClient.connect({ host: '0.0.0.0', port: port }, (error, client) => {
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
    })

});