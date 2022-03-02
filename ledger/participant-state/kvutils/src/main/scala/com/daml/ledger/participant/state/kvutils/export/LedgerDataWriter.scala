// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

trait LedgerDataWriter {
  def write(submissionInfo: SubmissionInfo, writeSet: WriteSet): Unit
}
