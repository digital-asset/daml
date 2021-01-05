// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

trait LedgerDataWriter {
  def write(submissionInfo: SubmissionInfo, writeSet: Seq[(Key, Value)]): Unit
}
