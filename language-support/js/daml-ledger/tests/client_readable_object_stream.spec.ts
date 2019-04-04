// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { expect, assert } from 'chai';
import * as grpc from 'daml-grpc';
import { MockedClientReadableStream } from './mock/client_readable_stream';
import * as mapping from '../src/mapping';
import * as call from '../src/call';

describe("ClientReadableObjectStream", () => {

    it("should support empty stream", (done) => {
        
        const wrapped = MockedClientReadableStream.with([]);
        const wrapper = call.ClientReadableObjectStream.from(wrapped, mapping.Identifier);

        let counter = 0;
        wrapper.on('data', (_id) => {
            counter = counter + 1;
        });

        wrapper.on('end', () => {
            expect(counter).to.equal(0);
            done();
        });

    });

    it("should read the expected items out of the wrapped stream", (done) => {

        const id1 = new grpc.Identifier();
        id1.setName('firstName');
        id1.setPackageId('firstPackageId');
        const id2 = new grpc.Identifier();
        id2.setName('secondName');
        id2.setPackageId('secondPackageId');

        const fixture = [id1, id2];

        const wrapped = MockedClientReadableStream.with(fixture);
        const wrapper = call.ClientReadableObjectStream.from(wrapped, mapping.Identifier);

        let counter = 0;
        wrapper.on('data', (id) => {
            expect(id).to.haveOwnProperty('name');
            expect(id).to.haveOwnProperty('packageId');
            expect(id).to.deep.equal(mapping.Identifier.toObject(fixture[counter]));
            counter = counter + 1;
        });

        wrapper.on('end', () => {
            expect(counter).to.equal(2);
            done();
        });

    });

    it('should correctly wrap an error', (done) => {
        const wrapper = call.ClientReadableObjectStream.from(new Error('hello, error'));
        let passed = false;
        wrapper.on('data', (_data) => {
            done(new Error('received unexpected data'));
        });
        wrapper.on('error', (error) => {
            expect(error.message).to.equal('hello, error');
            passed = true;
        });
        wrapper.on('end', () => {
            assert(passed);
            done();
        })
    });

});
