// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetPackageResponse: mapping.Mapping<grpc.GetPackageResponse, ledger.GetPackageResponse> = {
    toObject(response: grpc.GetPackageResponse): ledger.GetPackageResponse {
        return {
            hash: response.getHash(),
            hashFunction: ledger.HashFunction.SHA256,
            archivePayload: response.getArchivePayload_asB64()
        }
    },
    toMessage(response: ledger.GetPackageResponse): grpc.GetPackageResponse {
        const result = new grpc.GetPackageResponse();
        result.setHash(response.hash);
        result.setHashFunction(response.hashFunction);
        result.setArchivePayload(response.archivePayload);
        return result;
    }
}