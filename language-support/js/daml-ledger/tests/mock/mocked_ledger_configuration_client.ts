// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as sinon from 'sinon';

import * as grpc from 'daml-grpc';

import { CallOptions, ClientReadableStream, Metadata } from 'grpc';
import { MockedClientReadableStream } from './client_readable_stream';

export class MockedLedgerConfigurationClient implements grpc.ILedgerConfigurationClient {

    private responses: grpc.GetLedgerConfigurationResponse[]
    private readonly latestRequestSpy: sinon.SinonSpy

    constructor(responses: grpc.GetLedgerConfigurationResponse[], latestRequestSpy: sinon.SinonSpy) {
        this.responses = responses;
        this.latestRequestSpy = latestRequestSpy;
    }

    getLedgerConfiguration(request: grpc.GetLedgerConfigurationRequest, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.GetLedgerConfigurationResponse>;
    getLedgerConfiguration(request: grpc.GetLedgerConfigurationRequest, metadata?: Metadata | undefined, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.GetLedgerConfigurationResponse>;
    getLedgerConfiguration(request: grpc.GetLedgerConfigurationRequest, _metadata?: any, _options?: any) {
        this.latestRequestSpy(request);
        return MockedClientReadableStream.with(this.responses);
    }


}