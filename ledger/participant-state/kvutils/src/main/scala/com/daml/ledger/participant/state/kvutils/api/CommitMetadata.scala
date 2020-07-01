// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

/**
  * Exposes metadata about the commit.
  * The methods may lazily evaluate.
  */
sealed trait CommitMetadata {

  /**
    * @return estimated interpretation cost for a transaction; None in case of non-transaction
    *         submissions
    */
  def estimatedInterpretationCost: Option[Long]
}

object CommitMetadata {
  val Empty: CommitMetadata =
    SimpleCommitMetadata(estimatedInterpretationCost = None)
}

final case class SimpleCommitMetadata(override val estimatedInterpretationCost: Option[Long])
    extends CommitMetadata
