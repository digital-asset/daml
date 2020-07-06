// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.ledger.EventId
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.transaction.Node.{KeyWithMaintainers, NodeCreate, NodeExercises, NodeFetch}
import com.daml.lf.value.Value.{ContractId, ValueParty, ValueRecord, VersionedValue}
import com.daml.lf.value.ValueVersions
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Inside, LoneElement, Matchers}

import scala.collection.immutable.HashMap

private[dao] trait JdbcLedgerDaoDivulgenceSpec extends LoneElement with Inside {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private final val someVersionedCreateArg = VersionedValue(
    ValueVersions.acceptedVersions.head,
    ValueRecord(
      Some(someRecordId),
      ImmArray(Some(Ref.Name.assertFromString("field")) -> someValueText)))

  behavior of "JdbcLedgerDao (divulgence)"

  it should "preserve divulged contracts" in {
    val (create1, tx1) = {
      val nid = EventId.assertFromString("#0:0")
      val cid = ContractId.assertFromString("#contract1")
      val tx: GenTransaction.WithTxValue[EventId, ContractId] = GenTransaction(
        nodes = HashMap(
          nid ->
            NodeCreate(
              coid = cid,
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(alice),
              stakeholders = Set(alice),
              key = None
            )
        ),
        roots = ImmArray(nid),
      )
      cid -> tx
    }
    val (create2, tx2) = {
      val nid = EventId.assertFromString("#1:0")
      val cid = ContractId.assertFromString("#contract2")
      val tx: GenTransaction.WithTxValue[EventId, ContractId] = GenTransaction(
        nodes = HashMap(
          nid -> NodeCreate[ContractId, VersionedValue[ContractId]](
            coid = cid,
            coinst = someContractInstance,
            optLocation = None,
            signatories = Set(bob),
            stakeholders = Set(bob),
            key = Some(
              KeyWithMaintainers(
                VersionedValue(ValueVersions.acceptedVersions.head, ValueParty(bob)),
                Set(bob))
            )
          )
        ),
        roots = ImmArray(nid)
      )
      cid -> tx
    }
    val tx3: GenTransaction.WithTxValue[EventId, ContractId] = {
      val nidRootExercise = EventId.assertFromString("#2:0")
      val nidFetch = EventId.assertFromString("#2:1")
      val nidNestedExercise = EventId.assertFromString("#2:2")
      val nidCreate = EventId.assertFromString("#2:3")
      val cid = ContractId.assertFromString("#contract3")
      GenTransaction(
        nodes = HashMap(
          nidRootExercise ->
            NodeExercises[EventId, ContractId, VersionedValue[ContractId]](
              targetCoid = create1,
              templateId = someTemplateId,
              choiceId = Ref.ChoiceName.assertFromString("SomeChoice"),
              optLocation = None,
              consuming = true,
              actingParties = Set(bob),
              chosenValue = someVersionedCreateArg,
              stakeholders = Set(alice, bob),
              signatories = Set(alice),
              children = ImmArray(nidFetch, nidNestedExercise),
              exerciseResult = None,
              key = None,
            ),
          nidFetch ->
            NodeFetch[ContractId, VersionedValue[ContractId]](
              coid = create2,
              templateId = someTemplateId,
              optLocation = None,
              actingParties = Some(Set(bob)),
              signatories = Set(bob),
              stakeholders = Set(bob),
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueParty(bob)),
                  Set(bob))
              ),
            ),
          nidNestedExercise ->
            NodeExercises[EventId, ContractId, VersionedValue[ContractId]](
              targetCoid = create2,
              templateId = someTemplateId,
              choiceId = Ref.ChoiceName.assertFromString("SomeChoice"),
              optLocation = None,
              consuming = true,
              actingParties = Set(bob),
              chosenValue = someVersionedCreateArg,
              stakeholders = Set(bob),
              signatories = Set(bob),
              children = ImmArray(nidCreate),
              exerciseResult = None,
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueParty(bob)),
                  Set(bob))),
            ),
          nidCreate ->
            NodeCreate[ContractId, VersionedValue[ContractId]](
              coid = cid,
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(bob),
              stakeholders = Set(alice, bob),
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueParty(bob)),
                  Set(bob))
              )
            ),
        ),
        roots = ImmArray(nidRootExercise),
      )
    }

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
        divulgedContracts = Map((create2, someContractInstance) -> Set(alice)),
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
