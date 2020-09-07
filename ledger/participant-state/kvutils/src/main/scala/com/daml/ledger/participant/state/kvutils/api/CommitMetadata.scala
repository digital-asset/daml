// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Bytes, KeyValueCommitting}
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy

import scala.collection.JavaConverters._

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

  def inputKeys: Iterable[Bytes]

  def outputKeys: Iterable[Bytes]
}

object CommitMetadata {
  val Empty: CommitMetadata =
    SimpleCommitMetadata(estimatedInterpretationCost = None)

  def fromSubmission(
      submission: DamlSubmission,
      inputEstimatedInterpretationCost: Option[Long]): CommitMetadata = new CommitMetadata {
    override def estimatedInterpretationCost: Option[Long] = inputEstimatedInterpretationCost

    override def inputKeys: Iterable[Bytes] =
      submission.getInputDamlStateList.asScala
        .map(DefaultStateKeySerializationStrategy.serializeStateKey)

    override def outputKeys: Iterable[Bytes] =
      KeyValueCommitting
        .submissionOutputs(submission)
        .map(DefaultStateKeySerializationStrategy.serializeStateKey)
  }
}

final case class SimpleCommitMetadata(override val estimatedInterpretationCost: Option[Long])
    extends CommitMetadata {
  override def inputKeys: Iterable[Bytes] = throw new NotImplementedError()

  override def outputKeys: Iterable[Bytes] = throw new NotImplementedError()
}
