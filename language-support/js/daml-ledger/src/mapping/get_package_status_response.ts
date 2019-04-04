// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetPackageStatusResponse: mapping.Mapping<grpc.GetPackageStatusResponse, ledger.GetPackageStatusResponse> = {
    toObject(response: grpc.GetPackageStatusResponse): ledger.GetPackageStatusResponse {
        return {
            status: mapping.PackageStatus.toObject(response.getPackageStatus())
        };
    },
    toMessage(response: ledger.GetPackageStatusResponse): grpc.GetPackageStatusResponse {
        const result = new grpc.GetPackageStatusResponse();
        result.setPackageStatus(mapping.PackageStatus.toMessage(response.status));
        return result;
    }
}