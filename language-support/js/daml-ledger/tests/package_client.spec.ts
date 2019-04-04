// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { test } from 'mocha';
import { expect, assert } from 'chai';
import * as sinon from 'sinon';
import { MockedPackageClient } from './mock';
import * as grpc from 'daml-grpc';
import * as ledger from '../src';

describe("PackageClient", () => {

    const ledgerId = 'some-cool-id';
    const latestRequestSpy = sinon.spy();
    const packageData: [string, [ledger.GetPackageResponse, ledger.GetPackageStatusResponse]][] = [
        ['package-1', [{ hashFunction: 0, hash: 'cafebabe', archivePayload: 'cafebabe' }, { status: 0 }]],
        ['package-2', [{ hashFunction: 0, hash: 'deadbeef', archivePayload: 'deadbeef' }, { status: 1 }]]
    ];

    const ps = new MockedPackageClient(packageData, latestRequestSpy);
    const pc = new ledger.PackageClient(ledgerId, ps);

    afterEach(() => {
        sinon.restore();
        latestRequestSpy.resetHistory();
    });

    test("[7.1] When supported packages are requested, it returns them from the ledger", (done) => {
        pc.listPackages((error, response) => {
            expect(error).to.be.null;
            expect(response!.packageIds).to.deep.equal(['package-1', 'package-2']);
            done();
        });
    });

    test("[7.2] The supported packages are requested with the correct ledger ID", (done) => {
        pc.listPackages((error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.ListPackagesRequest);
            const request = latestRequestSpy.lastCall.lastArg as grpc.ListPackagesRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            done();
        });
    });

    test('[7.3] When a package is requested, it is returned from the ledger (present)', (done) => {
        pc.getPackage('package-2', (error, response) => {
            expect(error).to.be.null;
            expect(response).to.deep.equal({ hashFunction: 0, hash: 'deadbeef', archivePayload: 'deadbeef' });
            done();
        });
    });

    test('[7.3] When a package is requested, it is returned from the ledger (absent)', (done) => {
        pc.getPackage('package-3', (error, response) => {
            expect(response).to.be.undefined;
            expect(error).to.not.be.null;
            expect(error!.message).to.contain('NOT_FOUND');
            done();
        });
    });

    test("[7.4] The package is requested with the correct ledger ID", (done) => {
        pc.getPackage('package-2', (error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetPackageRequest);
            const request = latestRequestSpy.lastCall.lastArg as grpc.GetPackageRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            expect(request.getPackageId()).to.equal('package-2');
            done();
        });
    });

    test('[7.5] When the status of a package is requested, it is returned from the ledger (present)', (done) => {
        pc.getPackageStatus('package-2', (error, response) => {
            expect(error).to.be.null;
            expect(response).to.deep.equal({ status: 1 });
            done();
        });
    });

    test('[7.5] When the status of a package is requested, it is returned from the ledger (absent)', (done) => {
        pc.getPackageStatus('package-3', (error, response) => {
            expect(response).to.be.undefined;
            expect(error).to.not.be.null;
            expect(error!.message).to.contain('NOT_FOUND');
            done();
        });
    });

    test("[7.6] The status is requested with the correct ledger ID", (done) => {
        pc.getPackageStatus('package-2', (error, _response) => {
            expect(error).to.be.null;
            assert(latestRequestSpy.calledOnce, 'The latestRequestSpy has not been called exactly once');
            expect(latestRequestSpy.lastCall.args).to.have.length(1);
            expect(latestRequestSpy.lastCall.lastArg).to.be.an.instanceof(grpc.GetPackageStatusRequest);
            const request = latestRequestSpy.lastCall.lastArg as grpc.GetPackageStatusRequest;
            expect(request.getLedgerId()).to.equal(ledgerId);
            expect(request.getPackageId()).to.equal('package-2');
            done();
        });
    });

});
