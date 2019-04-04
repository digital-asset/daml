// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const GetPackageResponse: jsc.Arbitrary<ledger.GetPackageResponse> =
    jsc.tuple([jsc.elements([ledger.HashFunction.SHA256]), jsc.string, jsc.string]).smap<ledger.GetPackageResponse>(
        ([hashFunction, hash, archivePayload]) => {
            return {
                hashFunction: hashFunction,
                hash: hash,
                archivePayload: archivePayload
            }
        },
        (request) => {
            return [request.hashFunction, request.hash, request.archivePayload];
        }
    );