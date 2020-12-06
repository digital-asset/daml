// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate, NodeExercises, NodeFetch}
import com.daml.lf.transaction.TransactionVersions
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractInst, ValueParty, VersionedValue}
import com.daml.lf.value.ValueVersion
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{Inside, LoneElement}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[dao] trait JdbcLedgerDaoDivulgenceSpec extends LoneElement with Inside {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (divulgence)"

  it should "preserve divulged contracts" in {
    val (create1, tx1) = {
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      builder.add(
        NodeCreate(
          coid = contractId,
          coinst = someContractInstance,
          optLocation = None,
          signatories = Set(alice),
          stakeholders = Set(alice),
          key = None,
          version = TransactionVersions.minVersion,
        )
      )
      contractId -> builder.buildCommitted()
    }
    val (create2, tx2) = {
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      builder.add(
        NodeCreate(
          coid = contractId,
          coinst = someContractInstance,
          optLocation = None,
          signatories = Set(bob),
          stakeholders = Set(bob),
          key = Some(
            KeyWithMaintainers(ValueParty(bob), Set(bob))
          ),
          version = TransactionVersions.minVersion,
        )
      )
      contractId -> builder.buildCommitted()
    }
    val tx3 = {
      val builder = TransactionBuilder()
      val rootExercise = builder.add(
        NodeExercises(
          targetCoid = create1,
          templateId = someTemplateId,
          choiceId = Ref.ChoiceName.assertFromString("SomeChoice"),
          optLocation = None,
          consuming = true,
          actingParties = Set(bob),
          chosenValue = someValueRecord,
          stakeholders = Set(alice, bob),
          signatories = Set(alice),
          choiceObservers = Set.empty, //FIXME #7709, also test the case of non-empty choice-observers
          children = ImmArray.empty,
          exerciseResult = None,
          key = None,
          byKey = false,
          version = TransactionVersions.minVersion,
        )
      )
      builder.add(
        NodeFetch(
          coid = create2,
          templateId = someTemplateId,
          optLocation = None,
          actingParties = Set(bob),
          signatories = Set(bob),
          stakeholders = Set(bob),
          key = Some(
            KeyWithMaintainers(ValueParty(bob), Set(bob))
          ),
          byKey = false,
          version = TransactionVersions.minVersion,
        ),
        parentId = rootExercise,
      )
      val nestedExercise = builder.add(
        NodeExercises(
          targetCoid = create2,
          templateId = someTemplateId,
          choiceId = Ref.ChoiceName.assertFromString("SomeChoice"),
          optLocation = None,
          consuming = true,
          actingParties = Set(bob),
          chosenValue = someValueRecord,
          stakeholders = Set(bob),
          signatories = Set(bob),
          choiceObservers = Set.empty,
          children = ImmArray.empty,
          exerciseResult = None,
          key = Some(
            KeyWithMaintainers(ValueParty(bob), Set(bob))
          ),
          byKey = false,
          version = TransactionVersions.minVersion,
        ),
        parentId = rootExercise,
      )
      builder.add(
        NodeCreate(
          coid = builder.newCid,
          coinst = someContractInstance,
          optLocation = None,
          signatories = Set(bob),
          stakeholders = Set(alice, bob),
          key = Some(
            KeyWithMaintainers(ValueParty(bob), Set(bob))
          ),
          version = TransactionVersions.minVersion,
        ),
        parentId = nestedExercise,
      )
      builder.buildCommitted()
    }

    val someVersionedContractInstance =
      ContractInst(
        template = someContractInstance.template,
        agreementText = someContractInstance.agreementText,
        arg = VersionedValue(
          version = ValueVersion("6"),
          value = someContractInstance.arg
        )
      )

    val t1 = Instant.now()
    val t2 = t1.plusMillis(1)
    val t3 = t2.plusMillis(1)
    val appId = UUID.randomUUID.toString
    for {
      _ <- store(
        nextOffset() -> LedgerEntry.Transaction(
          commandId = Some(UUID.randomUUID.toString),
          transactionId = UUID.randomUUID.toString,
          applicationId = Some(appId),
          actAs = List(alice),
          workflowId = None,
          ledgerEffectiveTime = t1,
          recordedAt = t1,
          transaction = tx1,
          explicitDisclosure = Map.empty,
        )
      )
      _ <- store(
        nextOffset() -> LedgerEntry.Transaction(
          commandId = Some(UUID.randomUUID.toString),
          transactionId = UUID.randomUUID.toString,
          applicationId = Some(appId),
          actAs = List(bob),
          workflowId = None,
          ledgerEffectiveTime = t2,
          recordedAt = t2,
          transaction = tx2,
          explicitDisclosure = Map.empty,
        )
      )
      _ <- store(
        divulgedContracts = Map((create2, someVersionedContractInstance) -> Set(alice)),
        blindingInfo = None,
        offsetAndTx = nextOffset() -> LedgerEntry.Transaction(
          commandId = Some(UUID.randomUUID.toString),
          transactionId = UUID.randomUUID.toString,
          applicationId = Some(appId),
          actAs = List(bob),
          workflowId = None,
          ledgerEffectiveTime = t3,
          recordedAt = t3,
          transaction = tx3,
          explicitDisclosure = Map.empty,
        )
      )
    } yield {
      succeed
    }
  }

}
