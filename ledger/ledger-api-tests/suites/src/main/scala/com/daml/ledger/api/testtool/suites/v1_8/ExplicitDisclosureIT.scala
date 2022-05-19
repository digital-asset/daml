// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.commands.DisclosedContract
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.client.binding
import com.daml.ledger.test.model.Test._

import scala.concurrent.{ExecutionContext, Future}

final class ExplicitDisclosureIT extends LedgerTestSuite {
  import ExplicitDisclosureIT._

  test(
    "EDMetadata",
    "All create events have metadata defined",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    val contractKey = ledger.nextKeyId()
    for {
      _ <- ledger.create(owner, Delegated(owner, contractKey))
      _ <- ledger.create(owner, Delegation(owner, delegate))
      flats <- ledger.flatTransactions(owner)
      trees <- ledger.transactionTrees(owner)
      someTransactionId = flats.head.transactionId
      flatById <- ledger.flatTransactionById(someTransactionId, owner)
      treeById <- ledger.transactionTreeById(someTransactionId, owner)
    } yield {
      assertLength("flatTransactions", 2, flats)
      assertLength("transactionTrees", 2, trees)
      assert(
        flats.map(createdEvents).forall(_.forall(_.metadata.isDefined)),
        "Metadata is empty for flatTransactions",
      )
      assert(
        trees.map(createdEvents).forall(_.forall(_.metadata.isDefined)),
        "Metadata is empty for transactionTrees",
      )
      assert(
        createdEvents(flatById).forall(_.metadata.isDefined),
        "Metadata is empty for flatTransactionById",
      )
      assert(
        createdEvents(treeById).forall(_.metadata.isDefined),
        "Metadata is empty for transactionTreeById",
      )
    }
  })

  test(
    "EDCorrectDisclosure",
    "Submission works if the correct disclosure is provided",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      // Exercise a choice on the Delegation that fetches the Delegated contract
      // Fails because the submitter doesn't see the contract being fetched
      exerciseFetchError <- testContext.exerciseFetchDelegated().failed

      // Exercise the same choice, this time using correct explicit disclosure
      _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)

      // Create an extra disclosed contract
      extraKey = ledger.nextKeyId()
      _ <- ledger.create(owner, Delegated(owner, extraKey))
      delegatedTxs <- ledger.flatTransactionsByTemplateId(Delegated.id, owner)
      extraDelegatedEvent = createdEvents(delegatedTxs(1)).head
      extraDisclosedContract = createEventToDisclosedContract(extraDelegatedEvent)

      // Exercise the same choice, this time with superfluous disclosure
      _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract, extraDisclosedContract)
    } yield {
      assertGrpcError(
        exerciseFetchError,
        LedgerApiErrors.ConsistencyErrors.ContractNotFound,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "EDArchivedDisclosedContracts",
    "The ledger rejects archived disclosed contracts",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      // Archive the disclosed contract
      _ <- ledger.exercise(owner, testContext.delegatedCid.exerciseArchive(_))

      // Exercise the choice using the now inactive disclosed contract
      exerciseError <- testContext.exerciseFetchDelegated(testContext.disclosedContract).failed
    } yield {
      assertGrpcError(
        exerciseError,
        LedgerApiErrors.ConsistencyErrors.ContractNotFound,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "EDInvalidDisclosedContracts",
    "The ledger rejects invalid disclosed contracts",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      // Exercise a choice using invalid explicit disclosure (bad contract key)
      errorBadKey <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.arguments := Delegated(owner, "wrongKey").arguments)
        )
        .failed

      // Exercise a choice using invalid explicit disclosure (bad ledger time)
      errorBadLet <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.metadata.createdAt := com.google.protobuf.timestamp.Timestamp.of(1, 0))
        )
        .failed

      // Exercise a choice using invalid explicit disclosure (bad payload)
      errorBadPayload <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.arguments := Delegated(delegate, testContext.contractKey).arguments)
        )
        .failed
    } yield {
      assertGrpcError(
        errorBadKey,
        LedgerApiErrors.ConsistencyErrors.DisclosedContractInvalid,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorBadLet,
        LedgerApiErrors.ConsistencyErrors.DisclosedContractInvalid,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorBadPayload,
        LedgerApiErrors.ConsistencyErrors.DisclosedContractInvalid,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}

object ExplicitDisclosureIT {
  case class TestContext(
      ledger: ParticipantTestContext,
      owner: binding.Primitive.Party,
      delegate: binding.Primitive.Party,
      contractKey: String,
      delegationCid: binding.Primitive.ContractId[Delegation],
      delegatedCid: binding.Primitive.ContractId[Delegated],
      originalCreateEvent: CreatedEvent,
      disclosedContract: DisclosedContract,
  ) {

    /** Exercises the FetchDelegated choice as the delegate party, with the given explicit disclosure contracts.
      * This choice fetches the Delegation contract which is only visible to the owner.
      */
    def exerciseFetchDelegated(disclosedContracts: DisclosedContract*): Future[Unit] = {
      val request = ledger
        .submitAndWaitRequest(
          delegate,
          delegationCid.exerciseFetchDelegated(delegate, delegatedCid).command,
        )
        .update(_.commands.disclosedContracts := disclosedContracts)
      ledger.submitAndWait(request)
    }
  }

  def initializeTest(
      ledger: ParticipantTestContext,
      owner: binding.Primitive.Party,
      delegate: binding.Primitive.Party,
  )(implicit ec: ExecutionContext): Future[TestContext] = {
    val contractKey = ledger.nextKeyId()

    for {
      // Create a Delegation contract
      // Contract is visible both to owner (as signatory) and delegate (as observer)
      delegationCid <- ledger.create(owner, Delegation(owner, delegate))

      // Create Delegated contract
      // This contract is only visible to the owner
      delegatedCid <- ledger.create(owner, Delegated(owner, contractKey))

      // Get the contract payload from the transaction stream of the owner
      delegatedTx <- ledger.flatTransactionsByTemplateId(Delegated.id, owner)
      createDelegatedEvent = createdEvents(delegatedTx.head).head

      // Copy the actual Delegated contract to a disclosed contract (which can be shared out of band).
      disclosedContract = createEventToDisclosedContract(createDelegatedEvent)
    } yield TestContext(
      ledger = ledger,
      owner = owner,
      delegate = delegate,
      contractKey = contractKey,
      delegationCid = delegationCid,
      delegatedCid = delegatedCid,
      originalCreateEvent = createDelegatedEvent,
      disclosedContract = disclosedContract,
    )
  }

  def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract = DisclosedContract(
    templateId = ev.templateId,
    contractId = ev.contractId,
    arguments = ev.createArguments,
    metadata = ev.metadata,
  )
}
