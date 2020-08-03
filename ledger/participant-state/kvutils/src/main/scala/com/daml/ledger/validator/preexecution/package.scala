// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.lf.data.Time.Timestamp

package object preexecution {

  /**
    * A raw key-value pair with a flag indicating whether the key is a log entry key or not.
    */
  type AnnotatedRawKeyValuePairs = Seq[(AnnotatedRawKey, Bytes)]

  /**
    * Produces the record time on the participant for updates originating from pre-executed submissions.
    */
  type TimeUpdatesProvider = () => Option[Timestamp]
}
