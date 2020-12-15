// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.lf.data.Time.Timestamp

package object preexecution {

  /**
    * Produces the record time on the participant for updates originating from pre-executed submissions.
    */
  type TimeUpdatesProvider =
    () => Option[Timestamp]
}
