// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.domain

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta, Update}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.Transaction.{KeyInput => TxKeyInput}
import com.daml.lf.transaction.{BlindingInfo, SubmittedTransaction}
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.appendonlydao.events.Key

import scala.concurrent.Future

sealed trait PreparedSubmission extends Product with Serializable

final case class PreparedTransactionSubmission(
    keyInputs: Map[Key, TxKeyInput],
    inputContracts: Set[events.ContractId],
    updatedKeys: Map[Key, Option[events.ContractId]],
    consumedContracts: Set[events.ContractId],
    originalTx: Submission.Transaction,
) extends PreparedSubmission

final case class NoOpPreparedSubmission[T <: Submission](payload: T) extends PreparedSubmission

sealed trait Submission extends Product with Serializable
object Submission {
  type Validated[T] = Either[Update.CommandRejected, T]
  type AsyncValidationStep[T] = Future[Validated[T]]

  final case class Transaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  ) extends Submission {
    val blindingInfo: BlindingInfo = Blinding.blind(transaction)
  }
  final case class Config(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  ) extends Submission
  final case class AllocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  ) extends Submission
  final case class UploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  ) extends Submission
}
