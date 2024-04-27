// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.Id
import com.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.protocol.{
  LfCommittedTransaction,
  LfHash,
  LfTransaction,
  LfTransactionVersion,
  LfVersionedTransaction,
}

/** Default values for objects from the Daml repo for unit testing */
object DefaultDamlValues {
  def lfApplicationId(index: Int = 0): Ref.ApplicationId =
    Ref.ApplicationId.assertFromString(s"application-id-$index")
  def applicationId(index: Int = 0): ApplicationId = ApplicationId(lfApplicationId(index))

  def lfCommandId(index: Int = 0): Ref.CommandId =
    Ref.CommandId.assertFromString(s"command-id-$index")
  def commandId(index: Int = 0): CommandId = CommandId(lfCommandId(index))

  def submissionId(index: Int = 0): LedgerSubmissionId =
    LedgerSubmissionId.assertFromString(s"submission-id-$index")

  lazy val deduplicationDuration: DeduplicationDuration = DeduplicationDuration(
    java.time.Duration.ofSeconds(100)
  )

  def lfTransactionId(index: Int): Ref.TransactionId =
    Ref.TransactionId.assertFromString(s"lf-transaction-id-$index")

  def lfhash(index: Int = 0): LfHash = {
    val bytes = new Array[Byte](32)
    for (i <- 0 to 3) {
      bytes(i) = (index >>> (24 - i * 8)).toByte
    }
    LfHash.assertFromByteArray(bytes)
  }

  lazy val emptyTransaction: LfTransaction =
    LfTransaction(nodes = Map.empty, roots = ImmArray.empty)
  lazy val emptyVersionedTransaction: LfVersionedTransaction =
    LfVersionedTransaction(LfTransactionVersion.VDev, Map.empty, ImmArray.empty)
  lazy val emptyCommittedTransaction: LfCommittedTransaction =
    LfCommittedTransaction.subst[Id](emptyVersionedTransaction)
}
