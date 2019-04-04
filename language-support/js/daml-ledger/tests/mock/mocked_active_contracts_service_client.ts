// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import { ClientReadableStream } from 'grpc';
import { MockedClientReadableStream } from './client_readable_stream';
import { SinonSpy } from 'sinon';

export class MockedActiveContractsServiceClient implements grpc.IActiveContractsClient {

    private responses: grpc.GetActiveContractsResponse[]
    private readonly lastRequestSpy: SinonSpy

    constructor(responses: grpc.GetActiveContractsResponse[], lastRequestSpy: SinonSpy) {
        this.responses = responses;
        this.lastRequestSpy = lastRequestSpy;
    }

    getActiveContracts(request: grpc.GetActiveContractsRequest): ClientReadableStream<grpc.GetActiveContractsResponse> {
        this.lastRequestSpy(request);
        return MockedClientReadableStream.with(this.responses);
    }

}