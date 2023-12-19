// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.test.{TransactionBuilder, TreeTransactionBuilder}
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, Node, TransactionVersion, Versioned}
import com.daml.lf.value.Value.{ContractInstance, ValueParty}
import com.digitalasset.canton.platform.store.entries.LedgerEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement}

import java.util.UUID

import TreeTransactionBuilder.*

private[dao] trait JdbcLedgerDaoDivulgenceSpec extends LoneElement with Inside {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (divulgence)"

  it should "preserve divulged contracts" in {
    val (create1, tx1) = {
      val contractId = TransactionBuilder.newCid
      val create =
        Node.Create(
          coid = contractId,
          templateId = someTemplateId,
          arg = someContractArgument,
          agreementText = someAgreement,
          signatories = Set(alice),
          stakeholders = Set(alice),
          keyOpt = None,
          version = TransactionVersion.minVersion,
        )
      contractId -> TreeTransactionBuilder.toCommittedTransaction(create)
    }
    val (create2Cid, tx2) = {
      val contractId = TransactionBuilder.newCid
      val create =
        Node.Create(
          coid = contractId,
          templateId = someTemplateId,
          arg = someContractArgument,
          agreementText = someAgreement,
          signatories = Set(bob),
          stakeholders = Set(bob),
          keyOpt = Some(
            GlobalKeyWithMaintainers
              .assertBuild(
                someTemplateId,
                someContractKey(bob, "some key"),
                Set(bob),
                shared = true,
              )
          ),
          version = TransactionVersion.minVersion,
        )
      contractId -> TreeTransactionBuilder.toCommittedTransaction(create)
    }
    val tx3 = {
      val exercise3a = Node.Exercise(
        targetCoid = create1,
        templateId = someTemplateId,
        interfaceId = None,
        choiceId = someChoiceName,
        consuming = true,
        actingParties = Set(bob),
        chosenValue = someChoiceArgument,
        stakeholders = Set(alice, bob),
        signatories = Set(alice),
        choiceObservers = Set.empty,
        choiceAuthorizers = None,
        children = ImmArray.Empty,
        exerciseResult = Some(someChoiceResult),
        keyOpt = None,
        byKey = false,
        version = TransactionVersion.minVersion,
      )

      val fetch3b = Node.Fetch(
        coid = create2Cid,
        templateId = someTemplateId,
        actingParties = Set(bob),
        signatories = Set(bob),
        stakeholders = Set(bob),
        keyOpt = Some(
          GlobalKeyWithMaintainers
            .assertBuild(someTemplateId, ValueParty(bob), Set(bob), shared = true)
        ),
        byKey = false,
        version = TransactionVersion.minVersion,
      )

      val exercise3c = Node.Exercise(
        targetCoid = create2Cid,
        templateId = someTemplateId,
        interfaceId = None,
        choiceId = someChoiceName,
        consuming = true,
        actingParties = Set(bob),
        chosenValue = someChoiceArgument,
        stakeholders = Set(bob),
        signatories = Set(bob),
        choiceObservers = Set.empty,
        choiceAuthorizers = None,
        children = ImmArray.Empty,
        exerciseResult = Some(someChoiceResult),
        keyOpt = Some(
          GlobalKeyWithMaintainers
            .assertBuild(someTemplateId, someContractKey(bob, "some key"), Set(bob), shared = true)
        ),
        byKey = false,
        version = TransactionVersion.minVersion,
      )

      val create3d = Node.Create(
        coid = TransactionBuilder.newCid,
        templateId = someTemplateId,
        arg = someContractArgument,
        agreementText = someAgreement,
        signatories = Set(bob),
        stakeholders = Set(alice, bob),
        keyOpt = Some(
          GlobalKeyWithMaintainers
            .assertBuild(someTemplateId, someContractKey(bob, "some key"), Set(bob), shared = true)
        ),
        version = TransactionVersion.minVersion,
      )

      TreeTransactionBuilder.toCommittedTransaction(
        exercise3a.withChildren(
          fetch3b,
          exercise3c.withChildren(
            create3d
          ),
        )
      )
    }

    val someVersionedContractInstance =
      Versioned(
        TransactionVersion.V14,
        ContractInstance(
          template = someContractInstance.template,
          arg = someContractInstance.arg,
        ),
      )

    val t1 = Timestamp.now()
    val t2 = t1.addMicros(1000)
    val t3 = t2.addMicros(1000)
    val appId: Ref.ApplicationId = UUID.randomUUID().toString
    for {
      _ <- store(
        nextOffset() -> LedgerEntry.Transaction(
          commandId = Some(UUID.randomUUID().toString),
          transactionId = UUID.randomUUID().toString,
          applicationId = Some(appId),
          submissionId = Some(UUID.randomUUID().toString),
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
          commandId = Some(UUID.randomUUID().toString),
          transactionId = UUID.randomUUID().toString,
          applicationId = Some(appId),
          submissionId = Some(UUID.randomUUID().toString),
          actAs = List(bob),
          workflowId = None,
          ledgerEffectiveTime = t2,
          recordedAt = t2,
          transaction = tx2,
          explicitDisclosure = Map.empty,
        )
      )
      _ <- store(
        divulgedContracts = Map((create2Cid, someVersionedContractInstance) -> Set(alice)),
        blindingInfo = None,
        offsetAndTx = nextOffset() -> LedgerEntry.Transaction(
          commandId = Some(UUID.randomUUID().toString),
          transactionId = UUID.randomUUID().toString,
          applicationId = Some(appId),
          submissionId = Some(UUID.randomUUID().toString),
          actAs = List(bob),
          workflowId = None,
          ledgerEffectiveTime = t3,
          recordedAt = t3,
          transaction = tx3,
          explicitDisclosure = Map.empty,
        ),
      )
    } yield {
      succeed
    }
  }

}
