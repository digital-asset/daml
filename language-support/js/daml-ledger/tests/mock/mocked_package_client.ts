// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ClientUnaryCall, Metadata, CallOptions } from 'grpc';

import * as grpc from 'daml-grpc';
import { MockedClientUnaryCall } from './client_unary_call';
import * as ledger from '../../src';
import * as mapping from '../../src/mapping';

import * as sinon from 'sinon';

export class MockedPackageClient implements grpc.IPackageClient {

    private static NOT_FOUND_ERROR = new Error('NOT_FOUND');

    private readonly data: Map<string, [ledger.GetPackageResponse, ledger.GetPackageStatusResponse]>;
    private readonly listPackagesResponse: grpc.ListPackagesResponse;
    private readonly latestRequestSpy: sinon.SinonSpy;

    constructor(data: [ string, [ledger.GetPackageResponse, ledger.GetPackageStatusResponse]][], latestRequestSpy: sinon.SinonSpy) {
        this.data = new Map(data);
        this.latestRequestSpy = latestRequestSpy;
        this.listPackagesResponse = new grpc.ListPackagesResponse();
        this.data.forEach((_, packageId) => {
            this.listPackagesResponse.addPackageIds(packageId);
        });
    }

    listPackages(request: grpc.ListPackagesRequest, callback: (error: Error | null, response: grpc.ListPackagesResponse) => void): ClientUnaryCall;
    listPackages(request: grpc.ListPackagesRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.ListPackagesResponse) => void): ClientUnaryCall;
    listPackages(request: grpc.ListPackagesRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.ListPackagesResponse) => void): ClientUnaryCall;
    listPackages(request: grpc.ListPackagesRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback; 
        cb(null, this.listPackagesResponse);
        return MockedClientUnaryCall.Instance;
    }
    getPackage(request: grpc.GetPackageRequest, callback: (error: Error | null, response: grpc.GetPackageResponse) => void): ClientUnaryCall;
    getPackage(request: grpc.GetPackageRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.GetPackageResponse) => void): ClientUnaryCall;
    getPackage(request: grpc.GetPackageRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.GetPackageResponse) => void): ClientUnaryCall;
    getPackage(request: grpc.GetPackageRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback; 
        const packageId = request.getPackageId();
        if (this.data.has(packageId)) {
            cb(null, mapping.GetPackageResponse.toMessage(this.data.get(packageId)![0]));
        } else {
            cb(MockedPackageClient.NOT_FOUND_ERROR, null);
        }
        return MockedClientUnaryCall.Instance;
    }
    getPackageStatus(request: grpc.GetPackageStatusRequest, callback: (error: Error | null, response: grpc.GetPackageStatusResponse) => void): ClientUnaryCall;
    getPackageStatus(request: grpc.GetPackageStatusRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.GetPackageStatusResponse) => void): ClientUnaryCall;
    getPackageStatus(request: grpc.GetPackageStatusRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.GetPackageStatusResponse) => void): ClientUnaryCall;
    getPackageStatus(request: grpc.GetPackageStatusRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback;
        const packageId = request.getPackageId();
        if (this.data.has(packageId)) {
            cb(null, mapping.GetPackageStatusResponse.toMessage(this.data.get(packageId)![1]));
        } else {
            cb(MockedPackageClient.NOT_FOUND_ERROR, null);
        }
        return MockedClientUnaryCall.Instance;
    }

}
