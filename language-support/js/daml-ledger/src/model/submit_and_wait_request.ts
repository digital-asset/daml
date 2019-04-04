// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ledger.SubmitAndWaitRequest
 */
export interface SubmitAndWaitRequest {
    /**
     * @member {ledger.Commands} commands
     * @memberof ledger.SubmitAndWaitRequest
     * @instance
     */
    commands: ledger.Commands
}