// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ledger.SubmitRequest
 */
export interface SubmitRequest {
    /**
     * @member {ledger.Commands} commands
     * @memberof ledger.SubmitRequest
     * @instance
     */
    commands: ledger.Commands
}