// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import cats.implicits.{catsStdInstancesForFuture, toFunctorOps}
import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.testtool.suites.v2_1.CompanionImplicits.*
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.CreateCommand
import com.daml.ledger.test.java.model.test.{Delegated, Delegation, DiscloseCreate, Dummy}
import com.digitalasset.canton.ledger.error.groups.{
  CommandExecutionErrors,
  ConsistencyErrors,
  RequestValidationErrors,
}
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.google.protobuf.ByteString
import org.scalatest.Inside.inside

import java.util.List as JList
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps
import scala.util.{Success, Try}

final class ExplicitDisclosureIT extends LedgerTestSuite {
  import ExplicitDisclosureIT.*

  test(
    "EDCorrectCreatedEventBlobDisclosure",
    "Submission is successful if the correct disclosure as created_event_blob is provided",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        // Ensure participants are synchronized
        _ <- p.synchronize

        // Exercise a choice on the Delegation that fetches the Delegated contract
        // Fails because the submitter doesn't see the contract being fetched
        exerciseFetchError <- testContext
          .exerciseFetchDelegated()
          .mustFail("the submitter does not see the contract")

        // Exercise the same choice, this time using correct explicit disclosure
        _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)
      } yield {
        assertEquals(!testContext.disclosedContract.createdEventBlob.isEmpty, true)

        assertGrpcError(
          exerciseFetchError,
          ConsistencyErrors.ContractNotFound,
          None,
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "EDSuperfluousDisclosure",
    "Submission is successful when unnecessary disclosed contract is provided",
    allocate(SingleParty, SingleParty),
  )(testCase = implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        dummyCid: Dummy.ContractId <- ownerParticipant.create(owner, new Dummy(owner))
        txReq <- ownerParticipant.getTransactionsRequest(
          formatByPartyAndTemplate(owner, Dummy.TEMPLATE_ID)
        )
        dummyTxs <- ownerParticipant.transactions(
          txReq
        )
        dummyCreate = createdEvents(dummyTxs(0)).headOption.value
        dummyDisclosedContract = createEventToDisclosedContract(dummyCreate)

        // Ensure participants are synchronized
        _ <- p.synchronize

        // Exercise works with provided disclosed contract
        _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)
        // Exercise works with the Dummy contract as a superfluous disclosed contract
        _ <- testContext.exerciseFetchDelegated(
          testContext.disclosedContract,
          dummyDisclosedContract,
        )

        // Archive the Dummy contract
        _ <- ownerParticipant.exercise(owner, dummyCid.exerciseArchive())

        // Ensure participants are synchronized
        _ <- p.synchronize

        // Exercise works with the archived superfluous disclosed contract
        _ <- testContext.exerciseFetchDelegated(
          testContext.disclosedContract,
          dummyDisclosedContract,
        )
      } yield ()
  })

  test(
    "EDArchivedDisclosedContracts",
    "The ledger rejects archived disclosed contracts",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        // Archive the disclosed contract
        _ <- ownerParticipant.exercise(owner, testContext.delegatedCid.exerciseArchive())

        // Ensure participants are synchronized
        _ <- p.synchronize

        // Exercise the choice using the now inactive disclosed contract
        _ <- testContext
          .exerciseFetchDelegated(testContext.disclosedContract)
          .mustFail("the contract is already archived")
      } yield {
        // TODO(#16361) ED: Assert specific error codes once Canton error codes can be accessible from these suites
      }
  })

  test(
    "EDDisclosedContractsArchiveRaceTest",
    "Only one archival succeeds in a race between a normal exercise and one with disclosed contracts",
    allocate(SingleParty, SingleParty),
    repeated = 3,
  )(implicit ec => {
    case p @ Participants(Participant(ledger1, Seq(party1)), Participant(ledger2, Seq(party2))) =>
      val attempts = 10

      Future
        .traverse((1 to attempts).toList) {
          _ =>
            for {
              contractId: Dummy.ContractId <- ledger1.create(party1, new Dummy(party1))

              transactions <- ledger1
                .transactionsByTemplateId(Dummy.TEMPLATE_ID, Some(Seq(party1)))
              create = createdEvents(transactions(0)).headOption.value
              disclosedContract = createEventToDisclosedContract(create)

              // Submit concurrently two consuming exercise choices (with and without disclosed contract)
              party1_exerciseF = ledger1.exercise(party1, contractId.exerciseArchive())
              // Ensure participants are synchronized
              _ <- p.synchronize
              party2_exerciseWithDisclosureF =
                ledger2.submitAndWait(
                  ledger2
                    .submitAndWaitRequest(party2, contractId.exercisePublicChoice(party2).commands)
                    .update(_.commands.disclosedContracts := scala.Seq(disclosedContract))
                )

              // Wait for both commands to finish
              party1_exercise_result <- party1_exerciseF.transform(Success(_))
              party2_exerciseWithDisclosure <- party2_exerciseWithDisclosureF.transform(Success(_))
            } yield {
              oneFailedWith(
                party1_exercise_result,
                party2_exerciseWithDisclosure,
              ) { _ =>
                // TODO(#16361) ED: Assert specific error codes once Canton error codes can be accessible from these suites
                ()
              }
            }
        }
        .map(_ => ())
  })

  test(
    "EDMalformedDisclosedContractCreatedEventBlob",
    "The ledger rejects disclosed contracts with a malformed created event blob",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        // Ensure participants are synchronized
        _ <- p.synchronize

        // Exercise a choice using invalid explicit disclosure
        failure <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(_.createdEventBlob.set(ByteString.copyFromUtf8("foo")))
          )
          .mustFail("using a malformed disclosed contract created event blob")

      } yield {
        assertGrpcError(
          failure,
          RequestValidationErrors.InvalidArgument,
          Some(
            "The submitted request has invalid arguments: Unable to decode disclosed contract event payload: DecodeError"
          ),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "EDInconsistentDisclosedContract",
    "The ledger rejects inconsistent disclosed contract",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        ownerContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        // Create a new context only for the sake of getting a new disclosed contract
        // with the same template id
        delegateContext <- initializeTest(
          ownerParticipant = delegateParticipant,
          delegateParticipant = delegateParticipant,
          owner = delegate,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(delegate),
        )

        // Ensure participants are synchronized
        _ <- p.synchronize

        otherSalt = TransactionCoder
          .decodeFatContractInstance(delegateContext.disclosedContract.createdEventBlob)
          .map(_.authenticationData)
          .getOrElse(fail("contract decode failed"))

        tamperedEventBlob = TransactionCoder
          .encodeFatContractInstance(
            TransactionCoder
              .decodeFatContractInstance(ownerContext.disclosedContract.createdEventBlob)
              .map(_.setAuthenticationData(otherSalt))
              .getOrElse(fail("contract decode failed"))
          )
          .getOrElse(fail("contract encode failed"))

        _ <- ownerContext
          // Use of inconsistent disclosed contract
          // i.e. the delegate cannot fetch the owner's contract with attaching a different disclosed contract
          .exerciseFetchDelegated(
            ownerContext.disclosedContract.copy(createdEventBlob = tamperedEventBlob)
          )
          .mustFail("using an inconsistent disclosed contract created event blob")
      } yield {
        // TODO(#16361) ED: Assert specific error codes once Canton error codes can be accessible from these suites
        //          Should be DISCLOSED_CONTRACT_AUTHENTICATION_FAILED
      }
  })

  test(
    "EDDuplicates",
    "Submission is accepted on duplicate contract ids or key hashes with the same payload",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        // Ensure participants are synchronized
        _ <- p.synchronize

        // Exercise a choice with a disclosed contract
        _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)

        // Submission with disclosed contracts with the same contract id and same payload should be accepted
        _ <- testContext
          .dummyCreate(testContext.disclosedContract, testContext.disclosedContract)
      } yield ()
  })

  test(
    "EDRejectOnCreatedEventBlobNotSet",
    "Submission is rejected when the disclosed contract created event blob is not set",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFormat = formatByPartyAndTemplate(owner),
        )

        // Ensure participants are synchronized
        _ <- p.synchronize

        failure <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract.copy(
              createdEventBlob = ByteString.EMPTY
            )
          )
          .mustFail("Submitter forwarded a contract with unpopulated created_event_blob")
      } yield {
        assertGrpcError(
          failure,
          RequestValidationErrors.MissingField,
          Some(
            "The submitted command is missing a mandatory field: DisclosedContract.createdEventBlob"
          ),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "EDDiscloseeUsesWitnessedContract",
    "A contract creation witness can fetch and use it as a disclosed contract in a command submission (if authorized)",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(witnessParticipant, Seq(witness)),
        ) =>
      for {
        discloseCreate <- ownerParticipant.create(owner, new DiscloseCreate(owner))
        _ <- ownerParticipant.exercise(owner, discloseCreate.exerciseDiscloseCreate_To(witness))
        _ <- p.synchronize
        end <- witnessParticipant.currentEnd()
        req = ownerParticipant.getUpdatesRequestWithEnd(
          transactionFormatO = Some(formatByPartyAndTemplate(witness, Dummy.TEMPLATE_ID)),
          begin = witnessParticipant.begin,
          end = Some(end),
        )
        witnessTxs <- witnessParticipant.transactions(req)
        tx = assertSingleton("Witness' transactions", witnessTxs)
        witnessedCreate = assertSingleton("The witnessed create", createdEvents(tx))
        witnessedCreateAsDisclosedContract = createEventToDisclosedContract(witnessedCreate)
        _ = assert(
          !witnessedCreateAsDisclosedContract.createdEventBlob.isEmpty,
          "createdEventBlob is empty",
        )
        dummyContract = new Dummy.ContractId(witnessedCreateAsDisclosedContract.contractId)
        _ <- {
          val request = witnessParticipant
            .submitAndWaitRequest(
              witness,
              dummyContract.exercisePublicChoice(witness).commands,
            )
            .update(_.commands.disclosedContracts := Seq(witnessedCreateAsDisclosedContract))
          witnessParticipant.submitAndWait(request).map(_ => ())
        }
      } yield ()
  })

  test(
    "EDFailOnDisclosedContractIdMismatchWithPrescribedSynchronizerId",
    "A submission is rejected if an attached disclosed contract specifies a synchronizer id different than the synchronizer id prescribed in the command",
    allocate(SingleParty, SingleParty).expectingMinimumNumberOfSynchronizers(2),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      val (synchronizer1, synchronizer2) = inside(owner.initialSynchronizers) {
        case synchronizer1 :: synchronizer2 :: _ =>
          synchronizer1 -> synchronizer2
      }

      val contractKey = ownerParticipant.nextKeyId()
      for {
        delegationCid <- ownerParticipant
          .submitAndWaitForTransaction(
            ownerParticipant
              .submitAndWaitForTransactionRequest(
                owner,
                new Delegation(owner.getValue, delegate.getValue).create().commands(),
              )
              .update(_.commands.synchronizerId.set(synchronizer1))
          )
          .map(
            _.getTransaction.events.headOption.value.getCreated.contractId
              .pipe(new Delegation.ContractId(_))
          )

        delegatedCid <- ownerParticipant
          .submitAndWaitForTransaction(
            ownerParticipant
              .submitAndWaitForTransactionRequest(
                owner,
                new Delegated(owner.getValue, contractKey).create().commands(),
              )
              .update(_.commands.synchronizerId := synchronizer1)
          )
          .map(
            _.getTransaction.events.headOption.value.getCreated.contractId
              .pipe(new Delegated.ContractId(_))
          )

        // Get the contract payload from the transaction stream of the owner
        txReq <- ownerParticipant.getTransactionsRequest(formatByPartyAndTemplate(owner))
        delegatedTx <- ownerParticipant.transactions(txReq)
        createDelegatedEvent = createdEvents(delegatedTx.headOption.value).headOption.value

        // Copy the actual Delegated contract to a disclosed contract (which can be shared out of band).
        disclosedContract = createEventToDisclosedContract(createDelegatedEvent)

        // Ensure participants are synchronized
        _ <- p.synchronize

        request = delegateParticipant
          .submitAndWaitRequest(
            delegate,
            delegationCid.exerciseFetchDelegated(delegatedCid).commands,
          )
          .update(
            _.commands.disclosedContracts := Seq(
              disclosedContract.copy(synchronizerId = synchronizer2)
            )
          )
          .update(_.commands.synchronizerId := synchronizer1)
        error <- delegateParticipant
          .submitAndWait(request)
          .mustFail(
            "the disclosed contract has a different synchronizer id than the prescribed one"
          )
      } yield assertGrpcError(
        error,
        CommandExecutionErrors.PrescribedSynchronizerIdMismatch,
        None,
        checkDefiniteAnswerMetadata = true,
      )
  })

  test(
    "EDRouteByDisclosedContractSynchronizerId",
    "The synchronizer id of disclosed contracts is used as the prescribed synchronizer id",
    allocate(SingleParty, SingleParty).expectingMinimumNumberOfSynchronizers(2),
  )(implicit ec => {
    case p @ Participants(
          Participant(ownerParticipant, Seq(owner)),
          Participant(delegateParticipant, Seq(delegate)),
        ) =>
      val (synchronizer1, synchronizer2) = inside(owner.initialSynchronizers) {
        case synchronizer1 :: synchronizer2 :: _ =>
          synchronizer1 -> synchronizer2
      }

      for {
        _ <- testRoutingByDisclosedContractSynchronizerId(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          targetSynchronizer = synchronizer1,
          p,
        )
        _ <- testRoutingByDisclosedContractSynchronizerId(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          targetSynchronizer = synchronizer2,
          p,
        )
      } yield ()
  })

  private def oneFailedWith(result1: Try[?], result2: Try[?])(
      assertError: Throwable => Unit
  ): Unit =
    (result1.isFailure, result2.isFailure) match {
      case (true, false) => assertError(result1.failure.exception)
      case (false, true) => assertError(result2.failure.exception)
      case (true, true) => fail("Exactly one request should have failed, but both failed")
      case (false, false) => fail("Exactly one request should have failed, but both succeeded")
    }
}

object ExplicitDisclosureIT {
  final case class TestContext(
      ownerParticipant: ParticipantTestContext,
      delegateParticipant: ParticipantTestContext,
      owner: Party,
      delegate: Party,
      contractKey: String,
      delegationCid: Delegation.ContractId,
      delegatedCid: Delegated.ContractId,
      originalCreateEvent: CreatedEvent,
      disclosedContract: DisclosedContract,
  ) {

    /** Exercises the FetchDelegated choice as the delegate party, with the given explicit
      * disclosure contracts. This choice fetches the Delegation contract which is only visible to
      * the owner.
      */
    def exerciseFetchDelegated(
        disclosedContracts: DisclosedContract*
    )(implicit executionContext: ExecutionContext): Future[Unit] = {
      val request = delegateParticipant
        .submitAndWaitRequest(
          delegate,
          delegationCid.exerciseFetchDelegated(delegatedCid).commands,
        )
        .update(_.commands.disclosedContracts := disclosedContracts)
      delegateParticipant.submitAndWait(request).void
    }

    def dummyCreate(
        disclosedContracts: DisclosedContract*
    )(implicit executionContext: ExecutionContext): Future[Unit] = {
      val request = delegateParticipant
        .submitAndWaitRequest(
          delegate,
          JList.of(
            new CreateCommand(
              Dummy.TEMPLATE_ID,
              new Dummy(delegate.getValue).toValue,
            )
          ),
        )
        .update(_.commands.disclosedContracts := disclosedContracts)
      delegateParticipant.submitAndWait(request).void
    }

  }

  private def initializeTest(
      ownerParticipant: ParticipantTestContext,
      delegateParticipant: ParticipantTestContext,
      owner: Party,
      delegate: Party,
      transactionFormat: TransactionFormat,
  )(implicit ec: ExecutionContext): Future[TestContext] = {
    val contractKey = ownerParticipant.nextKeyId()

    for {
      // Create a Delegation contract
      // Contract is visible both to owner (as signatory) and delegate (as observer)
      delegationCid <- ownerParticipant.create(
        owner,
        new Delegation(owner.getValue, delegate.getValue),
      )

      // Create Delegated contract
      // This contract is only visible to the owner
      delegatedCid <- ownerParticipant.create(owner, new Delegated(owner.getValue, contractKey))

      // Get the contract payload from the transaction stream of the owner
      txReq <- ownerParticipant.getTransactionsRequest(transactionFormat)
      delegatedTx <- ownerParticipant.transactions(txReq)
      createDelegatedEvent = createdEvents(delegatedTx.headOption.value).headOption.value

      // Copy the actual Delegated contract to a disclosed contract (which can be shared out of band).
      disclosedContract = createEventToDisclosedContract(createDelegatedEvent)
    } yield TestContext(
      ownerParticipant = ownerParticipant,
      delegateParticipant = delegateParticipant,
      owner = owner,
      delegate = delegate,
      contractKey = contractKey,
      delegationCid = delegationCid,
      delegatedCid = delegatedCid,
      originalCreateEvent = createDelegatedEvent,
      disclosedContract = disclosedContract,
    )
  }

  private def formatByPartyAndTemplate(
      owner: Party,
      templateId: javaapi.data.Identifier = Delegated.TEMPLATE_ID,
  ): TransactionFormat = {
    val templateIdScalaPB = Identifier.fromJavaProto(templateId.toProto)

    TransactionFormat(
      Some(
        EventFormat(
          filtersByParty = Map(
            owner.getValue -> new Filters(
              Seq(
                CumulativeFilter(
                  IdentifierFilter.TemplateFilter(
                    TemplateFilter(Some(templateIdScalaPB), includeCreatedEventBlob = true)
                  )
                )
              )
            )
          ),
          filtersForAnyParty = None,
          verbose = false,
        )
      ),
      transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
    )
  }

  private def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract =
    DisclosedContract(
      templateId = ev.templateId,
      contractId = ev.contractId,
      createdEventBlob = ev.createdEventBlob,
      synchronizerId = "",
    )

  private def testRoutingByDisclosedContractSynchronizerId(
      ownerParticipant: ParticipantTestContext,
      delegateParticipant: ParticipantTestContext,
      owner: Party,
      delegate: Party,
      targetSynchronizer: String,
      p: Participants,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val contractKey = ownerParticipant.nextKeyId()

    for {
      delegationCid <- ownerParticipant
        .submitAndWaitForTransaction(
          ownerParticipant
            .submitAndWaitForTransactionRequest(
              owner,
              new Delegation(owner.getValue, delegate.getValue).create().commands(),
            )
        )
        .map(
          _.getTransaction.events.headOption.value.getCreated.contractId
            .pipe(new Delegation.ContractId(_))
        )

      (delegatedCid, delegatedCreateUpdateId) <- ownerParticipant
        .submitAndWaitForTransaction(
          ownerParticipant
            .submitAndWaitForTransactionRequest(
              owner,
              new Delegated(owner.getValue, contractKey).create().commands(),
            )
            .update(_.commands.synchronizerId := targetSynchronizer)
        )
        .map(resp =>
          resp.getTransaction.events.headOption.value.getCreated.contractId
            .pipe(new Delegated.ContractId(_)) -> resp.getTransaction.updateId
        )

      // Get the contract payload from the transaction stream of the owner
      txReq <- ownerParticipant.getTransactionsRequest(formatByPartyAndTemplate(owner))
      delegatedTx <- ownerParticipant
        .transactions(txReq)
        .map(_.filter(_.updateId == delegatedCreateUpdateId))
      createDelegatedEvent = createdEvents(delegatedTx.headOption.value).headOption.value

      // Copy the actual Delegated contract to a disclosed contract (which can be shared out of band).
      disclosedContract = createEventToDisclosedContract(createDelegatedEvent)

      // Ensure participants are synchronized
      _ <- p.synchronize

      request = delegateParticipant
        .submitAndWaitForTransactionRequest(
          delegate,
          delegationCid.exerciseFetchDelegated(delegatedCid).commands,
        )
        .update(
          _.commands.disclosedContracts := Seq(
            disclosedContract.copy(synchronizerId = targetSynchronizer)
          )
        )
      txSynchronizerId <- delegateParticipant
        .submitAndWaitForTransaction(request)
        .map(_.getTransaction.synchronizerId)
    } yield {
      assertEquals(txSynchronizerId, targetSynchronizer)
    }
  }
}
