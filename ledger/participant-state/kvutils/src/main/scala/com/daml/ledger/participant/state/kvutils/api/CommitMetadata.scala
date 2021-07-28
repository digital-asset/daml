// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{KeyValueCommitting, Raw}
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.jdk.CollectionConverters._

/** Exposes metadata about the commit.
  * The methods may lazily evaluate.
  */
sealed trait CommitMetadata {

  /** @return estimated interpretation cost for a transaction; None in case of non-transaction
    *         submissions
    */
  def estimatedInterpretationCost: Option[Long]

  def inputKeys(serializationStrategy: StateKeySerializationStrategy): Iterable[Raw.StateKey]

  def outputKeys(serializationStrategy: StateKeySerializationStrategy): Iterable[Raw.StateKey]
}

object CommitMetadata {
  val Empty: CommitMetadata =
    new CommitMetadata {
      override def estimatedInterpretationCost: Option[Long] = None

      override def inputKeys(
          serializationStrategy: StateKeySerializationStrategy
      ): Iterable[Raw.StateKey] = Iterable.empty

      override def outputKeys(
          serializationStrategy: StateKeySerializationStrategy
      ): Iterable[Raw.StateKey] = Iterable.empty
    }

  def apply(
      submission: DamlSubmission,
      inputEstimatedInterpretationCost: Option[Long],
  ): CommitMetadata = new CommitMetadata {
    override def estimatedInterpretationCost: Option[Long] = inputEstimatedInterpretationCost

    override def inputKeys(
        serializationStrategy: StateKeySerializationStrategy
    ): Iterable[Raw.StateKey] =
      submission.getInputDamlStateList.asScala
        .map(serializationStrategy.serializeStateKey)

    private lazy val submissionOutputs = KeyValueCommitting.submissionOutputs(submission)

    override def outputKeys(
        serializationStrategy: StateKeySerializationStrategy
    ): Iterable[Raw.StateKey] =
      submissionOutputs.map(serializationStrategy.serializeStateKey)
  }
}
