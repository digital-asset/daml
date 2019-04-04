// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const ListPackagesResponse: mapping.Mapping<grpc.ListPackagesResponse, ledger.ListPackagesResponse> = {
    toObject(response: grpc.ListPackagesResponse): ledger.ListPackagesResponse {
        return {
            packageIds: response.getPackageIdsList()
        }
    },
    toMessage(response: ledger.ListPackagesResponse): grpc.ListPackagesResponse {
        const result = new grpc.ListPackagesResponse();
        result.setPackageIdsList(response.packageIds);
        return result;
    }
}