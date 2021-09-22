// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.kv.TransactionNormalizer
import com.daml.lf.transaction.CommittedTransaction
import com.google.rpc.code.Code
import com.google.rpc.status.Status

/** Normalizes updates so that ones generated by different versions of the Daml SDK may be compared.
  * I.e., we just want to check the structured information.
  */
trait UpdateNormalizer {
  def normalize(update: Update): Update
}

object UpdateNormalizer {
  private val MandatoryNormalizers = List(
    RecordTimeNormalizer,
    ConfigurationChangeRejectionNormalizer,
  )

  def normalize(initialUpdate: Update, normalizers: Iterable[UpdateNormalizer]): Update = {
    (normalizers ++ MandatoryNormalizers).foldLeft(initialUpdate) { case (update, normalizer) =>
      normalizer.normalize(update)
    }
  }
}

/** Normalizes the expected and/or actual updates based on their counterparts.
  * Useful when independent update normalization is not possible
  * (e.g. ensuring that the content of the actual update is a super-set of the content of the expected update).
  */
trait PairwiseUpdateNormalizer {
  def normalize(expectedUpdate: Update, actualUpdate: Update): (Update, Update)
}

object PairwiseUpdateNormalizer {
  def normalize(
      expectedUpdate: Update,
      actualUpdate: Update,
      normalizers: Iterable[PairwiseUpdateNormalizer],
  ): (Update, Update) =
    normalizers.foldLeft(expectedUpdate -> actualUpdate) { case ((expected, actual), normalizer) =>
      normalizer.normalize(expected, actual)
    }
}

/** Ignores the record time set later by post-execution because it's unimportant.
  * We only care about the update type and content.
  */
object RecordTimeNormalizer extends UpdateNormalizer {
  private val RecordTime = Timestamp.MinValue

  override def normalize(update: Update): Update = update match {
    case u: Update.ConfigurationChanged => u.copy(recordTime = RecordTime)
    case u: Update.ConfigurationChangeRejected => u.copy(recordTime = RecordTime)
    case u: Update.PartyAddedToParticipant => u.copy(recordTime = RecordTime)
    case u: Update.PartyAllocationRejected => u.copy(recordTime = RecordTime)
    case u: Update.PublicPackageUpload => u.copy(recordTime = RecordTime)
    case u: Update.PublicPackageUploadRejected => u.copy(recordTime = RecordTime)
    case u: Update.TransactionAccepted => u.copy(recordTime = RecordTime)
    case u: Update.CommandRejected => u.copy(recordTime = RecordTime)
  }
}

/** The v1 to v2 migration makes it difficult to validate the rejection status
  */
object RejectionReasonNormalizer extends UpdateNormalizer {
  override def normalize(update: Update): Update = update match {
    case commandRejected @ Update.CommandRejected(_, _, _) =>
      val newReason = FinalReason(
        Status(
          Code.UNKNOWN.value
        )
      )
      commandRejected.copy(reasonTemplate = newReason)
    case _ => update
  }
}

object ConfigurationChangeRejectionNormalizer extends UpdateNormalizer {
  override def normalize(update: Update): Update = update match {
    case u: Update.ConfigurationChangeRejected => u.copy(rejectionReason = "")
    case _ => update
  }
}

/** We may not want to check blinding info as we haven't always populated these. */
object BlindingInfoNormalizer extends UpdateNormalizer {
  override def normalize(update: Update): Update = update match {
    case t: Update.TransactionAccepted => t.copy(blindingInfo = None)
    case _ => update
  }
}

/** We may not want to care about fetch and lookup-by-key nodes in the transaction tree attached to a
  * TransactionAccepted event as in some Daml SDK versions we are dropping them.
  */
object FetchAndLookupByKeyNodeNormalizer extends UpdateNormalizer {
  override def normalize(update: Update): Update = update match {
    case t: Update.TransactionAccepted =>
      t.copy(transaction = CommittedTransaction(TransactionNormalizer.normalize(t.transaction)))
    case _ => update
  }
}
