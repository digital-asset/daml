// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate, NodeExercises, NodeFetch}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractInst, ValueParty, VersionedValue}
import com.daml.lf.value.ValueVersion
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Inside, LoneElement, Matchers}

private[dao] trait JdbcLedgerDaoDivulgenceSpec extends LoneElement with Inside {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (divulgence)"

  it should "preserve divulged contracts" in {
    val (create1, tx1) = {
      val builder = new TransactionBuilder
      val contractId = builder.newCid
      builder.add(
        NodeCreate(
          coid = contractId,
          coinst = someContractInstance,
          optLocation = None,
          signatories = Set(alice),
          stakeholders = Set(alice),
          key = None
        )
      )
      contractId -> builder.buildCommitted()
    }
    val (create2, tx2) = {
      val builder = new TransactionBuilder
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
          )
        )
      )
      contractId -> builder.buildCommitted()
    }
    val tx3 = {
      val builder = new TransactionBuilder
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
          children = ImmArray.empty,
          exerciseResult = None,
          key = None,
        )
      )
      builder.add(
        NodeFetch(
          coid = create2,
          templateId = someTemplateId,
          optLocation = None,
          actingParties = Some(Set(bob)),
          signatories = Set(bob),
          stakeholders = Set(bob),
          key = Some(
            KeyWithMaintainers(ValueParty(bob), Set(bob))
          ),
        ),
        parent = rootExercise,
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
          children = ImmArray.empty,
          exerciseResult = None,
          key = Some(
            KeyWithMaintainers(ValueParty(bob), Set(bob))
          ),
        ),
        parent = rootExercise,
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
          )
        ),
        parent = nestedExercise,
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
          submittingParty = Some(alice),
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
          submittingParty = Some(bob),
          workflowId = None,
          ledgerEffectiveTime = t2,
          recordedAt = t2,
          transaction = tx2,
          explicitDisclosure = Map.empty,
        )
      )
      _ <- store(
        divulgedContracts = Map((create2, someVersionedContractInstance) -> Set(alice)),
        nextOffset() -> LedgerEntry.Transaction(
          commandId = Some(UUID.randomUUID.toString),
          transactionId = UUID.randomUUID.toString,
          applicationId = Some(appId),
          submittingParty = Some(bob),
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
