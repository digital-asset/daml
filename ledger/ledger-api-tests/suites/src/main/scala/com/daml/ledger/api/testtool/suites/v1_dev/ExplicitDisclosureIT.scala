// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.{Command, DisclosedContract, ExerciseByKeyCommand}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test._
import com.google.protobuf.ByteString
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

// TODO ED: Enable in Sandbox-on-X conformance test-suite
final class ExplicitDisclosureIT extends LedgerTestSuite {
  import ExplicitDisclosureIT._

  test(
    "EDCorrectDisclosure",
    "Submission is successful if the correct disclosure is provided",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      // Exercise a choice on the Delegation that fetches the Delegated contract
      // Fails because the submitter doesn't see the contract being fetched
      exerciseFetchError <- testContext
        .exerciseFetchDelegated()
        .mustFail("the submitter does not see the contract")

      // Exercise the same choice, this time using correct explicit disclosure
      _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)
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
    "EDSuperfluousDisclosure",
    "Submission is successful when unnecessary disclosed contract is provided",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      dummyCid <- ledger.create(owner, Dummy(owner))
      dummyTxs <- ledger.flatTransactionsByTemplateId(Dummy.id, owner)
      dummyCreate = createdEvents(dummyTxs(0)).head
      dummyDisclosedContract = createEventToDisclosedContract(dummyCreate)

      // Exercise works with provided disclosed contract
      _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)
      // Exercise works with the Dummy contract as a superfluous disclosed contract
      _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract, dummyDisclosedContract)

      // Archive the Dummy contract
      _ <- ledger.exercise(owner, dummyCid.exerciseArchive())

      // Exercise works with the archived superfluous disclosed contract
      _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract, dummyDisclosedContract)
    } yield ()
  })

  test(
    "EDExerciseByKeyDisclosedContract",
    "A disclosed contract can be exercised by key by non-witness reader if authorized",
    partyAllocation = allocate(TwoParties),
    enabled = _.explicitDisclosure,
  ) { implicit ec =>
    { case Participants(Participant(ledger, owner, divulgee)) =>
      for {
        // Create contract with `owner` as only stakeholder
        _ <- ledger.create(owner, WithKey(owner))
        withKeyTxIds <- ledger.flatTransactionsByTemplateId(WithKey.id, owner)
        withKeyCreate = createdEvents(withKeyTxIds(1)).head
        withKeyDisclosedContract = createEventToDisclosedContract(withKeyCreate)
        exerciseByKeyError <- ledger
          .submitAndWait(
            exerciseWithKey_byKey_request(ledger, owner, divulgee, None)
          )
          .mustFail("divulgee does not see the contract")
        // Assert that a random party can exercise the contract by key (if authorized)
        // when passing the disclosed contract to the submission
        _ <- ledger.submitAndWait(
          exerciseWithKey_byKey_request(ledger, owner, divulgee, Some(withKeyDisclosedContract))
        )
      } yield assertGrpcError(
        exerciseByKeyError,
        LedgerApiErrors.CommandExecution.Interpreter.LookupErrors.ContractKeyNotFound,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  }

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
      acs <- ledger.activeContracts(owner)
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
      assert(
        acs.forall(_.metadata.isDefined),
        "Metadata is empty for activeContracts",
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
      _ <- ledger.exercise(owner, testContext.delegatedCid.exerciseArchive())

      // Exercise the choice using the now inactive disclosed contract
      exerciseError <- testContext
        .exerciseFetchDelegated(testContext.disclosedContract)
        .mustFail("the contract is already archived")
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
    "EDInconsistentDisclosedContracts",
    "The ledger rejects disclosed contracts with inconsistent metadata",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      // Exercise a choice using invalid explicit disclosure (bad contract key)
      errorBadKey <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.metadata.contractKeyHash := ByteString.copyFromUtf8("badKeyMeta"))
        )
        .mustFail("using a mismatching contract key hash in metadata")

      // Exercise a choice using invalid explicit disclosure (bad ledger time)
      errorBadLet <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.metadata.createdAt := com.google.protobuf.timestamp.Timestamp.of(1, 0))
        )
        .mustFail("using a mismatching ledger time")

      // Exercise a choice using invalid explicit disclosure (bad payload)
      errorBadPayload <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.arguments := Delegated(delegate, testContext.contractKey).arguments)
        )
        .mustFail("using an invalid disclosed contract payload")
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

  test(
    "EDMalformedDisclosedContracts",
    "The ledger rejects malformed contract payloads",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      // This payload does not typecheck, it has different fields than the corresponding template
      malformedArgument = Record(None, Seq(RecordField("", Some(Value(Value.Sum.Bool(false))))))

      errorMalformedPayload <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.arguments := malformedArgument)
        )
        .mustFail("using a malformed contract argument")

      // Exercise a choice using an invalid disclosed contract (missing templateId)
      errorMissingTemplateId <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.modify(_.clearTemplateId))
        )
        .mustFail("using a disclosed contract with missing templateId")

      // Exercise a choice using an invalid disclosed contract (empty contractId)
      errorMissingContractId <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract
            .update(_.contractId := "")
        )
        .mustFail("using a disclosed contract with empty contractId")

      // Exercise a choice using an invalid disclosed contract (empty create arguments)
      errorMissingArguments <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract.update(_.modify(_.clearArguments))
        )
        .mustFail("using a disclosed contract with empty arguments")

      // Exercise a choice using an invalid disclosed contract (missing contract metadata)
      errorMissingMetadata <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract.update(_.modify(_.clearArguments))
        )
        .mustFail("using a disclosed contract with missing contract metadata")

      // Exercise a choice using an invalid disclosed contract (missing createdAt in contract metadata)
      errorMissingCreatedAt <- testContext
        .exerciseFetchDelegated(
          testContext.disclosedContract.update(_.metadata.modify(_.clearCreatedAt))
        )
        .mustFail("using a disclosed contract with missing createdAt in contract metadata")
    } yield {
      assertGrpcError(
        errorMalformedPayload,
        // TODO ED: Verify that this error code is good enough for the user
        //          and that it includes the contract id
        LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorMissingTemplateId,
        LedgerApiErrors.RequestValidation.MissingField,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorMissingContractId,
        LedgerApiErrors.RequestValidation.MissingField,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorMissingMetadata,
        LedgerApiErrors.RequestValidation.MissingField,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorMissingArguments,
        LedgerApiErrors.RequestValidation.MissingField,
        None,
        checkDefiniteAnswerMetadata = true,
      )
      assertGrpcError(
        errorMissingCreatedAt,
        LedgerApiErrors.RequestValidation.MissingField,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "EDNormalizedDisclosedContract",
    "Submission works if the provided disclosed contract is normalized",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, party)) =>
    disclosedContractNormalizationSubmissionTest(
      ledger = ledger,
      owner = owner,
      party = party,
      normalizedDisclosedContract = true,
    )
  })

  test(
    "EDNonNormalizedDisclosedContract",
    "Submission works if the provided disclosed contract is not normalized",
    allocate(Parties(2)),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, party)) =>
    disclosedContractNormalizationSubmissionTest(
      ledger = ledger,
      owner = owner,
      party = party,
      normalizedDisclosedContract = false,
    )
  })

  test(
    "EDFeatureDiasbled",
    "Submission when disclosed contracts provided on feature disabled",
    allocate(Parties(2)),
    enabled = feature => !feature.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    for {
      testContext <- initializeTest(ledger, owner, delegate)

      exerciseFetchError <- testContext
        .exerciseFetchDelegated(testContext.disclosedContract)
        .mustFail("explicit disclosure feature is disabled")
    } yield {
      assertGrpcError(
        exerciseFetchError,
        LedgerApiErrors.RequestValidation.InvalidField,
        None,
        checkDefiniteAnswerMetadata = true,
        throwable =>
          assertEquals(
            throwable.getMessage,
            "INVALID_ARGUMENT: INVALID_FIELD(8,EDFeatur): The submitted command has a field with invalid value: Invalid field disclosed_contracts: feature in development: disclosed_contracts should not be set",
          ),
      )
    }
  })

  private def disclosedContractNormalizationSubmissionTest(
      ledger: ParticipantTestContext,
      owner: binding.Primitive.Party,
      party: binding.Primitive.Party,
      normalizedDisclosedContract: Boolean,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      // Create contract with `owner` as only stakeholder
      _ <- ledger.create(owner, WithKey(owner))
      txs <- ledger.flatTransactions(
        new GetTransactionsRequest(
          ledgerId = ledger.ledgerId,
          begin = Some(ledger.referenceOffset),
          end = Some(ledger.end),
          filter = Some(ledger.transactionFilter(Seq(owner))),
          verbose = !normalizedDisclosedContract,
        )
      )
      createdEvent = createdEvents(txs(1)).head
      disclosedContract = createEventToDisclosedContract(createdEvent)

      _ <- ledger.submitAndWait(
        exerciseWithKey_byKey_request(ledger, owner, party, Some(disclosedContract))
      )
    } yield ()
  }
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
          delegationCid.exerciseFetchDelegated(delegatedCid).command,
        )
        .update(_.commands.disclosedContracts := disclosedContracts)
      ledger.submitAndWait(request)
    }
  }

  private def initializeTest(
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

  private def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract =
    DisclosedContract(
      templateId = ev.templateId,
      contractId = ev.contractId,
      arguments = ev.createArguments,
      metadata = ev.metadata,
    )

  private def exerciseWithKey_byKey_request(
      ledger: ParticipantTestContext,
      owner: Primitive.Party,
      party: Primitive.Party,
      withKeyDisclosedContract: Option[DisclosedContract],
  ): SubmitAndWaitRequest =
    ledger
      .submitAndWaitRequest(
        party,
        Command.of(
          Command.Command.ExerciseByKey(
            ExerciseByKeyCommand(
              Some(WithKey.id.unwrap),
              Option(Value(Value.Sum.Party(owner.unwrap))),
              "WithKey_NoOp",
              Option(Value(Value.Sum.Party(party.unwrap))),
            )
          )
        ),
      )
      .update(_.commands.disclosedContracts := withKeyDisclosedContract.iterator.toSeq)
}
