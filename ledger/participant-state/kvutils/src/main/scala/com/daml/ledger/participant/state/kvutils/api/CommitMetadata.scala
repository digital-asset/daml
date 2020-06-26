// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

sealed trait CommitMetadata {
  def estimatedInterpretationCost: Long
}

object CommitMetadata {
  val Empty: CommitMetadata = SimpleCommitMetadata(0)
}

final case class SimpleCommitMetadata(override val estimatedInterpretationCost: Long)
    extends CommitMetadata
