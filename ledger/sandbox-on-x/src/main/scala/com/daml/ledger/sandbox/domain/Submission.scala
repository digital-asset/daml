// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.domain

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.lf.command.DisclosedContract
import com.daml.lf.data.Ref.SubmissionId
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.{SubmittedTransaction, Versioned}
import com.daml.logging.LoggingContext

private[sandbox] sealed trait Submission extends Product with Serializable {
  def submissionId: Ref.SubmissionId
  def loggingContext: LoggingContext
}

private[sandbox] object Submission {
  final case class Transaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      disclosedContracts: ImmArray[Versioned[DisclosedContract]],
  )(implicit val loggingContext: LoggingContext)
      extends Submission {
    val submissionId: SubmissionId = {
      // TODO SoX production-ready: Make the submissionId non-optional
      // .get deemed safe since no transaction submission should have the submission id empty
      submitterInfo.submissionId.get
    }
  }

  final case class Config(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit val loggingContext: LoggingContext)
      extends Submission
  final case class AllocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit val loggingContext: LoggingContext)
      extends Submission

  final case class UploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  )(implicit val loggingContext: LoggingContext)
      extends Submission
}
