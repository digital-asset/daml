// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const PackageStatus: mapping.Mapping<grpc.PackageStatus, ledger.PackageStatus> = {
    toObject(status: grpc.PackageStatus): ledger.PackageStatus {
        if (status === grpc.PackageStatus.REGISTERED) {
            return ledger.PackageStatus.REGISTERED;
        } else if (status === grpc.PackageStatus.UNKNOWN) {
            return ledger.PackageStatus.UNKNOWN;
        } else {
            throw new Error(`Unknown PackageStatus ${status}`);
        }
    },
    toMessage(status: ledger.PackageStatus): grpc.PackageStatus {
        if (status === ledger.PackageStatus.REGISTERED) {
            return grpc.PackageStatus.REGISTERED;
        } else if (status === ledger.PackageStatus.UNKNOWN) {
            return grpc.PackageStatus.UNKNOWN;
        } else {
            throw new Error(`Unknown PackageStatus ${status}`);

        }
    }
}