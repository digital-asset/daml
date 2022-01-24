// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.TransactionNodeStatistics.EmptyActions
import com.daml.lf.transaction.{CommittedTransaction, TransactionNodeStatistics, TransactionVersion, VersionedTransaction}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UpdateToMeteringDbDtoSpec extends AnyWordSpec with Matchers {

  private val DbDtoEq: org.scalactic.Equality[DbDto] = {
    case (a: DbDto, b: DbDto) =>
      (a.productPrefix === b.productPrefix) &&
        (a.productArity == b.productArity) &&
        (a.productIterator zip b.productIterator).forall {
          case (x: Array[_], y: Array[_]) => x sameElements y
          case (Some(x: Array[_]), Some(y: Array[_])) => x sameElements y
          case (x, y) => x === y
        }
    case (_, _) => false
  }

  private val DbDtoSeqEq: org.scalactic.Equality[Seq[DbDto]] = {
    case (a: Seq[_], b: Seq[_]) =>
      a.size == b.size && a.zip(b).forall({ case (x, y) => DbDtoEq.areEqual(x, y) })
    case (_, _) => false
  }

  "UpdateMeteringToDbDto" should {

    val applicationId = Ref.ApplicationId.assertFromString("a0")

    val timestamp: Long = 12345

    val offset = Ref.HexString.assertFromString("02")
    val statistics = TransactionNodeStatistics(
      EmptyActions.copy(creates = 2),
      EmptyActions.copy(consumingExercisesByCid = 1),
    )

    val someHash = Hash.hashPrivateKey("p0")

    val someRecordTime = Time.Timestamp.assertFromString("2000-01-01T00:00:00.000000Z")

    val someCompletionInfo = state.CompletionInfo(
      actAs = Nil,
      applicationId = applicationId,
      commandId = Ref.CommandId.assertFromString("c0"),
      optDeduplicationPeriod = None,
      submissionId = None,
      statistics = Some(statistics),
    )
    val someTransactionMeta = state.TransactionMeta(
      ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
      workflowId = None,
      submissionTime = Time.Timestamp.assertFromLong(3),
      submissionSeed = someHash,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

    val someTransactionAccepted = state.Update.TransactionAccepted(
      optCompletionInfo = Some(someCompletionInfo),
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(
        VersionedTransaction(TransactionVersion.VDev, Map.empty, ImmArray.empty)
      ),
      transactionId = Ref.TransactionId.assertFromString("TransactionId"),
      recordTime = someRecordTime,
      divulgedContracts = List.empty,
      blindingInfo = None,
    )

    "extract transaction metering" in {

      val actual = UpdateToMeteringDbDto(clock = () => timestamp)(List((Offset.fromHexString(offset), someTransactionAccepted)).iterator)

      val expected: Vector[DbDto.TransactionMetering] = Vector(
        DbDto.TransactionMetering(
          application_id = applicationId,
          action_count = statistics.committed.actions + statistics.rolledBack.actions,
          from_timestamp = timestamp,
          to_timestamp = timestamp,
          from_ledger_offset = offset,
          to_ledger_offset = offset,
        )
      )

      actual should equal(expected)(decided by DbDtoSeqEq)

    }

  }



}