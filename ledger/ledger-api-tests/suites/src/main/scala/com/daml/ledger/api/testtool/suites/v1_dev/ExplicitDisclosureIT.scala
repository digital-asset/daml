// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_dev

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.DisclosedContract.{Arguments => ProtoArguments}
import com.daml.ledger.api.v1.commands.{Command, DisclosedContract, ExerciseByKeyCommand}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test._
import com.daml.ledger.test.modelext.TestExtension.IDelegated
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Bytes
import com.daml.lf.data.Ref.{DottedName, Identifier, PackageId, QualifiedName}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import scalaz.syntax.tag._

import java.time.temporal.ChronoUnit
import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

final class ExplicitDisclosureIT extends LedgerTestSuite {
  import ExplicitDisclosureIT._

  test(
    "EDCorrectDisclosure",
    "Submission is successful if the correct disclosure is provided",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

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
    "EDCorrectBlobDisclosure",
    "Submission is successful if the correct disclosure as Blob is provided",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(
            owner,
            template = None,
            interface = byInterface(includeCreateArgumentsBlob = true),
          ),
        )

        // Exercise a choice on the Delegation that fetches the Delegated contract
        // Fails because the submitter doesn't see the contract being fetched
        exerciseFetchError <- testContext
          .exerciseFetchDelegated()
          .mustFail("the submitter does not see the contract")

        // Exercise the same choice, this time using correct explicit disclosure
        _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)
      } yield {
        assertEquals(testContext.disclosedContract.arguments.isCreateArgumentsBlob, true)

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
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(testCase = implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        dummyCid <- ownerParticipant.create(owner, Dummy(owner))
        dummyTxs <- ownerParticipant.flatTransactionsByTemplateId(Dummy.id, owner)
        dummyCreate = createdEvents(dummyTxs(0)).head
        dummyDisclosedContract = createEventToDisclosedContract(dummyCreate)

        // Exercise works with provided disclosed contract
        _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)
        // Exercise works with the Dummy contract as a superfluous disclosed contract
        _ <- testContext.exerciseFetchDelegated(
          testContext.disclosedContract,
          dummyDisclosedContract,
        )

        // Archive the Dummy contract
        _ <- ownerParticipant.exercise(owner, dummyCid.exerciseArchive())

        // Exercise works with the archived superfluous disclosed contract
        _ <- testContext.exerciseFetchDelegated(
          testContext.disclosedContract,
          dummyDisclosedContract,
        )
      } yield ()
  })

  test(
    "EDExerciseByKeyDisclosedContract",
    "A disclosed contract can be exercised by key with non-witness readers if authorized",
    partyAllocation = allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  ) { implicit ec =>
    {
      case Participants(
            Participant(ownerParticipant, owner),
            Participant(divulgeeParticipant, divulgee),
          ) =>
        for {
          // Create contract with `owner` as only stakeholder
          response <- ownerParticipant.submitAndWaitForTransaction(
            ownerParticipant.submitAndWaitRequest(owner, WithKey(owner).create.command)
          )
          withKeyCreationTx = assertSingleton(
            context = "Transaction expected non-empty",
            as = response.transaction.toList,
          )
          withKeyCreate = createdEvents(withKeyCreationTx).head
          withKeyDisclosedContract = createEventToDisclosedContract(withKeyCreate)
          exerciseByKeyError <- divulgeeParticipant
            .submitAndWait(
              exerciseWithKey_byKey_request(divulgeeParticipant, owner, divulgee, None)
            )
            .mustFail("divulgee does not see the contract")
          // Assert that a random party can exercise the contract by key (if authorized)
          // when passing the disclosed contract to the submission
          _ <- divulgeeParticipant.submitAndWait(
            exerciseWithKey_byKey_request(
              divulgeeParticipant,
              owner,
              divulgee,
              Some(withKeyDisclosedContract),
            )
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
    "All create events have correctly-defined metadata",
    allocate(Parties(2)),
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
      assertDisclosedContractsMetadata[Transaction](
        streamType = "flatTransactions",
        expectedCount = 2,
        streamResponses = flats,
        toCreateEvents = createdEvents,
        toLedgerEffectiveTime = _.getEffectiveAt,
      )
      assertDisclosedContractsMetadata[TransactionTree](
        streamType = "transactionTrees",
        expectedCount = 2,
        streamResponses = trees,
        toCreateEvents = createdEvents,
        toLedgerEffectiveTime = _.getEffectiveAt,
      )
      assertDisclosedContractsMetadata[CreatedEvent](
        streamType = "activeContracts",
        expectedCount = 2,
        streamResponses = acs,
        toCreateEvents = Vector(_),
        // ACS does not have effectiveAt, so use the one from the other streams (should be the roughly same)
        toLedgerEffectiveTime = _ => trees.head.getEffectiveAt,
      )

      assertDisclosedContractsMetadata[Transaction](
        streamType = "flatTransactionById",
        expectedCount = 1,
        streamResponses = Vector(flatById),
        toCreateEvents = createdEvents,
        toLedgerEffectiveTime = _.getEffectiveAt,
      )

      assertDisclosedContractsMetadata[TransactionTree](
        streamType = "transactionTreeById",
        expectedCount = 1,
        streamResponses = Vector(treeById),
        toCreateEvents = createdEvents,
        toLedgerEffectiveTime = _.getEffectiveAt,
      )
    }
  })

  test(
    "EDArchivedDisclosedContracts",
    "The ledger rejects archived disclosed contracts",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        // Archive the disclosed contract
        _ <- ownerParticipant.exercise(owner, testContext.delegatedCid.exerciseArchive())

        // Exercise the choice using the now inactive disclosed contract
        _ <- testContext
          .exerciseFetchDelegated(testContext.disclosedContract)
          .mustFail("the contract is already archived")
      } yield {
        // TODO ED: Assert specific error codes once Canton error codes can be accessible from these suites
      }
  })

  test(
    "EDDisclosedContractsArchiveRaceTest",
    "Only one archival succeeds in a race between a normal exercise and one with disclosed contracts",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
    repeated = 3,
  )(implicit ec => {
    case Participants(Participant(ledger1, party1), Participant(ledger2, party2)) =>
      val attempts = 10

      Future
        .traverse((1 to attempts).toList) {
          _ =>
            for {
              contractId <- ledger1.create(party1, Dummy(party1))

              transactions <- ledger1.flatTransactionsByTemplateId(Dummy.id, party1)
              create = createdEvents(transactions(0)).head
              disclosedContract = createEventToDisclosedContract(create)

              // Submit concurrently two consuming exercise choices (with and without disclosed contract)
              party1_exerciseF = ledger1.exercise(party1, contractId.exerciseArchive())
              party2_exerciseWithDisclosureF =
                ledger2.submitAndWait(
                  ledger2
                    .submitAndWaitRequest(party2, contractId.exercisePublicChoice(party2).command)
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
                // TODO ED: Assert specific error codes once Canton error codes can be accessible from these suites
                ()
              }
            }
        }
        .map(_ => ())
  })

  test(
    "EDInconsistentDisclosedContracts",
    "The ledger rejects disclosed contracts with inconsistent metadata",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        //      // TODO ED: Enable once the check is implemented in command interpretation
        //      // Exercise a choice using invalid explicit disclosure (bad contract key)
        //      errorBadKey <- testContext
        //        .exerciseFetchDelegated(
        //          testContext.disclosedContract
        //            .update(_.metadata.contractKeyHash := ByteString.copyFromUtf8("BadKeyBadKeyBadKeyBadKeyBadKey00"))
        //        )
        //        .mustFail("using a mismatching contract key hash in metadata")

        // Exercise a choice using invalid explicit disclosure (bad ledger time)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(
                _.metadata.createdAt.modify(original => original.withSeconds(original.seconds + 1L))
              )
          )
          .mustFail("using a mismatching ledger time")

        // Exercise a choice using invalid explicit disclosure (bad payload)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(
                _.arguments := ProtoArguments.CreateArguments(
                  Delegated(delegate, testContext.contractKey).arguments
                )
              )
          )
          .mustFail("using an invalid disclosed contract payload")
      } yield {
        // TODO ED: Assert specific error codes once Canton error codes can be accessible from these suites
      }
  })

  test(
    "EDMalformedDisclosedContracts",
    "The ledger rejects malformed contract payloads",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        // This payload does not typecheck, it has different fields than the corresponding template
        malformedArgument = Record(
          None,
          scala.Seq(RecordField("", Some(Value(Value.Sum.Bool(false))))),
        )

        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(_.arguments := ProtoArguments.CreateArguments(malformedArgument))
          )
          .mustFail("using a malformed contract argument")

        // Exercise a choice using an invalid disclosed contract (missing templateId)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(_.modify(_.clearTemplateId))
          )
          .mustFail("using a disclosed contract with missing templateId")

        // Exercise a choice using an invalid disclosed contract (empty contractId)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(_.contractId := "")
          )
          .mustFail("using a disclosed contract with empty contractId")

        // Exercise a choice using an invalid disclosed contract (empty create arguments)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract.update(_.modify(_.clearArguments))
          )
          .mustFail("using a disclosed contract with empty arguments")

        // Exercise a choice using an invalid disclosed contract (missing contract metadata)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract.update(_.modify(_.clearMetadata))
          )
          .mustFail("using a disclosed contract with missing contract metadata")

        // Exercise a choice using an invalid disclosed contract (missing createdAt in contract metadata)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract.update(_.metadata.modify(_.clearCreatedAt))
          )
          .mustFail("using a disclosed contract with missing createdAt in contract metadata")
      } yield {
        // TODO ED: Assert specific error codes once Canton error codes can be accessible from these suites
      }
  })

  test(
    "EDInconsistentSuperfluousDisclosedContracts",
    "The ledger accepts superfluous disclosed contracts with mismatching payload",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        _ <- ownerParticipant.create(owner, Dummy(owner))
        dummyTxs <- ownerParticipant.flatTransactionsByTemplateId(Dummy.id, owner)
        dummyCreate = createdEvents(dummyTxs(0)).head
        dummyDisclosedContract = createEventToDisclosedContract(dummyCreate)

        // Exercise a choice using invalid explicit disclosure (bad contract key)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract,
            // Provide a superfluous disclosed contract with mismatching key hash
            dummyDisclosedContract
              .update(
                _.metadata.contractKeyHash := ByteString.copyFromUtf8(
                  "BadKeyBadKeyBadKeyBadKeyBadKey00"
                )
              ),
          )

        // Exercise a choice using invalid explicit disclosure (bad ledger time)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract,
            // Provide a superfluous disclosed contract with mismatching createdAt
            dummyDisclosedContract
              .update(_.metadata.createdAt := com.google.protobuf.timestamp.Timestamp.of(1, 0)),
          )

        // Exercise a choice using invalid explicit disclosure (bad payload)
        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract,
            // Provide a superfluous disclosed contract with mismatching contract arguments
            dummyDisclosedContract
              .update(_.arguments := ProtoArguments.CreateArguments(Dummy(delegate).arguments)),
          )
      } yield ()
  })

  test(
    "EDDuplicates",
    "Submission is rejected on duplicate contract ids or key hashes",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        // Exercise a choice with a disclosed contract
        _ <- testContext.exerciseFetchDelegated(testContext.disclosedContract)

        // Submission with disclosed contracts with the same contract id should be rejected
        errorDuplicateContractId <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract,
            testContext.disclosedContract.update(
              // Set distinct key hash
              _.metadata.contractKeyHash.set(ByteString.EMPTY)
            ),
          )
          .mustFail("duplicate contract id")

        // Submission with disclosed contracts with the same contract key hashes should be rejected
        errorDuplicateKey <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract,
            testContext.disclosedContract.update(
              // Set distinct contract id
              _.modify(_.withContractId("00" * 32 + "ff"))
            ),
          )
          .mustFail("duplicate key hash")
      } yield {
        assertGrpcError(
          errorDuplicateContractId,
          LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
          Some(
            s"Preprocessor encountered a duplicate disclosed contract ID ContractId(${testContext.disclosedContract.contractId})"
          ),
          checkDefiniteAnswerMetadata = true,
        )

        val expectedKeyHashString = {
          val bytes = Bytes.fromByteString(
            testContext.disclosedContract.metadata
              .getOrElse(fail("metadata not present"))
              .contractKeyHash
          )
          Hash.fromBytes(bytes).getOrElse(fail("Could not decode hash")).toHexString
        }
        assertGrpcErrorRegex(
          errorDuplicateKey,
          LedgerApiErrors.CommandExecution.Interpreter.InvalidArgumentInterpretationError,
          Some(
            Pattern.compile(
              s"Found duplicated contract keys in submitted disclosed contracts .* $expectedKeyHashString"
            )
          ),
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
    "EDFeatureDisabled",
    "Submission fails when disclosed contracts provided on feature disabled",
    allocate(SingleParty, SingleParty),
    enabled = feature => !feature.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        exerciseFetchError <- testContext
          .exerciseFetchDelegated(testContext.disclosedContract)
          .mustFail("explicit disclosure feature is disabled")
      } yield {
        assertGrpcError(
          exerciseFetchError,
          LedgerApiErrors.RequestValidation.InvalidField,
          Some(
            "Invalid field disclosed_contracts: feature in development: disclosed_contracts should not be set"
          ),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  // TODO ED: Extract this assertion in generic stream/ACS tests once the feature is deemed stable
  test(
    "EDContractDriverMetadata",
    "The contract driver metadata is present and consistent across all endpoints",
    allocate(SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => { case Participants(Participant(ledger, owner)) =>
    for {
      // Create Dummy contract
      _ <- ledger.create(owner, Dummy(owner))

      // Fetch the create across possible endpoints
      acs <- ledger.activeContracts(owner)

      flatTxs <- ledger.flatTransactions(owner)
      flatTx = assertSingleton("Only one flat transaction expected", flatTxs)
      flatTxById <- ledger.flatTransactionById(flatTx.transactionId, owner)

      txTrees <- ledger.transactionTrees(owner)
      txTree = assertSingleton("Only one flat transaction expected", txTrees)
      txTreeById <- ledger.transactionTreeById(txTree.transactionId, owner)

      // Extract the created event from results
      acsCreatedEvent = assertSingleton("Only one ACS created event expected", acs)
      flatTxCreatedEvent = assertSingleton(
        context = "Only one flat transaction create event expected",
        as = createdEvents(flatTx),
      )
      flatTxByIdCreatedEvent = assertSingleton(
        context = "Only one flat transaction by id create event expected",
        as = createdEvents(flatTxById),
      )
      txTreeCreatedEvent = assertSingleton(
        context = "Only one transaction tree create event expected",
        as = createdEvents(txTree),
      )
      txTreeByIdCreatedEvent = assertSingleton(
        context = "Only one transaction tree by id create event expected",
        as = createdEvents(txTreeById),
      )
    } yield {
      def assertDriverMetadata(createdEvent: CreatedEvent): ByteString =
        createdEvent.metadata.getOrElse(fail("Missing metadata")).driverMetadata

      val acsCreateDriverMetadata = assertDriverMetadata(acsCreatedEvent)
      assert(!acsCreateDriverMetadata.isEmpty)

      assertEquals(acsCreateDriverMetadata, assertDriverMetadata(flatTxCreatedEvent))
      assertEquals(acsCreateDriverMetadata, assertDriverMetadata(flatTxByIdCreatedEvent))
      assertEquals(acsCreateDriverMetadata, assertDriverMetadata(txTreeCreatedEvent))
      assertEquals(acsCreateDriverMetadata, assertDriverMetadata(txTreeByIdCreatedEvent))
    }
  })

  test(
    "EDIncorrectDriverMetadata",
    "Submission is rejected on invalid contract driver metadata",
    allocate(SingleParty, SingleParty),
    enabled = _.explicitDisclosure,
  )(implicit ec => {
    case Participants(
          Participant(ownerParticipant, owner),
          Participant(delegateParticipant, delegate),
        ) =>
      for {
        testContext <- initializeTest(
          ownerParticipant = ownerParticipant,
          delegateParticipant = delegateParticipant,
          owner = owner,
          delegate = delegate,
          transactionFilter = filterTxBy(owner, template = byTemplate, interface = None),
        )

        _ <- testContext
          .exerciseFetchDelegated(
            testContext.disclosedContract
              .update(_.metadata.driverMetadata.set(ByteString.copyFromUtf8("00aabbcc")))
          )
          .mustFail("Submitter forwarded a contract with invalid driver metadata")
      } yield {
        // TODO ED: Assert specific error codes once Canton error codes can be accessible from these suites
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
      createdEvent = createdEvents(txs(0)).head
      disclosedContract = createEventToDisclosedContract(createdEvent)

      _ <- ledger.submitAndWait(
        exerciseWithKey_byKey_request(ledger, owner, party, Some(disclosedContract))
      )
    } yield ()
  }

  private def assertDisclosedContractsMetadata[T](
      streamType: String,
      expectedCount: Int,
      streamResponses: Vector[T],
      toCreateEvents: T => Vector[CreatedEvent],
      toLedgerEffectiveTime: T => Timestamp,
  ): Unit = {
    assertLength(streamType, expectedCount, streamResponses)

    streamResponses.foreach { streamResponse =>
      val createsInResponse = toCreateEvents(streamResponse)
      assertSingleton("only one create event expected", createsInResponse)
      val create = createsInResponse.head

      create.metadata.fold(fail(s"Metadata not defined for $streamType")) { metadata =>
        metadata.createdAt.fold(fail(s"created_at not defined in metadata for $streamType")) {
          metadataCreatedAt =>
            val txLedgerEffectiveTime = toLedgerEffectiveTime(streamResponse).asJavaInstant
            // Assert that the two instants are within one second of each-other
            val createdAt = metadataCreatedAt.asJavaInstant
            assert(
              Math.abs(
                createdAt.until(txLedgerEffectiveTime, ChronoUnit.MILLIS)
              ) < 10000,
              s"The two instants should be within 10 seconds of each-other, but were: $createdAt vs $txLedgerEffectiveTime",
            )
        }

        create.contractKey
          .map { cKey =>
            val actualKeyHash = Hash.assertFromByteArray(metadata.contractKeyHash.toByteArray)
            val expectedKeyHash =
              Hash.assertHashContractKey(
                fromApiIdentifier(Delegated.id.unwrap)
                  .fold(
                    errStr => fail(s"Failed converting from API to LF Identifier: $errStr"),
                    identity,
                  ),
                NoLoggingValueValidator
                  .validateValue(cKey)
                  .fold(err => fail("Failed converting contract key value", err), identity),
              )

            assertEquals(
              context =
                "the contract key hash should match the key hash provided in the disclosed contract metadata",
              actual = actualKeyHash,
              expected = expectedKeyHash,
            )
          }
          .getOrElse(())
      }
    }
  }

  private def oneFailedWith(result1: Try[_], result2: Try[_])(
      assertError: Throwable => Unit
  ): Unit =
    (result1.isFailure, result2.isFailure) match {
      case (true, false) => assertError(result1.failed.get)
      case (false, true) => assertError(result2.failed.get)
      case (true, true) => fail("Exactly one request should have failed, but both failed")
      case (false, false) => fail("Exactly one request should have failed, but both succeeded")
    }
}

object ExplicitDisclosureIT {
  case class TestContext(
      ownerParticipant: ParticipantTestContext,
      delegateParticipant: ParticipantTestContext,
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
      val request = delegateParticipant
        .submitAndWaitRequest(
          delegate,
          delegationCid.exerciseFetchDelegated(delegatedCid).command,
        )
        .update(_.commands.disclosedContracts := disclosedContracts)
      delegateParticipant.submitAndWait(request)
    }
  }

  private def initializeTest(
      ownerParticipant: ParticipantTestContext,
      delegateParticipant: ParticipantTestContext,
      owner: binding.Primitive.Party,
      delegate: binding.Primitive.Party,
      transactionFilter: TransactionFilter,
  )(implicit ec: ExecutionContext): Future[TestContext] = {
    val contractKey = ownerParticipant.nextKeyId()

    for {
      // Create a Delegation contract
      // Contract is visible both to owner (as signatory) and delegate (as observer)
      delegationCid <- ownerParticipant.create(owner, Delegation(owner, delegate))

      // Create Delegated contract
      // This contract is only visible to the owner
      delegatedCid <- ownerParticipant.create(owner, Delegated(owner, contractKey))

      // Get the contract payload from the transaction stream of the owner
      delegatedTx <- ownerParticipant.flatTransactions(
        ownerParticipant.getTransactionsRequest(transactionFilter)
      )
      createDelegatedEvent = createdEvents(delegatedTx.head).head

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

  private def filterTxBy(
      owner: binding.Primitive.Party,
      template: Option[value.Identifier],
      interface: Option[InterfaceFilter],
  ): TransactionFilter =
    new TransactionFilter(
      Map(
        owner.unwrap -> new Filters(
          Some(
            InclusiveFilters(
              template.toList,
              interface.toList,
            )
          )
        )
      )
    )

  private def byTemplate: Option[value.Identifier] = Some(Delegated.id.unwrap)
  private def byInterface(includeCreateArgumentsBlob: Boolean): Option[InterfaceFilter] = Some(
    InterfaceFilter(
      Some(IDelegated.id.unwrap),
      includeInterfaceView = true,
      includeCreateArgumentsBlob = includeCreateArgumentsBlob,
    )
  )

  private def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract = {
    val arguments = (ev.createArguments, ev.createArgumentsBlob) match {
      case (Some(createArguments), _) =>
        DisclosedContract.Arguments.CreateArguments(createArguments)
      case (_, Some(createArgumentsBlob)) =>
        DisclosedContract.Arguments.CreateArgumentsBlob(createArgumentsBlob)
      case _ =>
        sys.error("createEvent arguments are empty")
    }
    DisclosedContract(
      templateId = ev.templateId,
      contractId = ev.contractId,
      arguments = arguments,
      metadata = ev.metadata,
    )
  }

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
              Option(Value(Value.Sum.Party(Party.unwrap(owner)))),
              "WithKey_NoOp",
              Option(
                Value(
                  Value.Sum.Record(
                    Record(
                      None,
                      List(RecordField("", Some(Value(Value.Sum.Party(Party.unwrap(party)))))),
                    )
                  )
                )
              ),
            )
          )
        ),
      )
      .update(_.commands.disclosedContracts := withKeyDisclosedContract.iterator.toSeq)

  // TODO Deduplicate with Converter.fromApiIdentifier
  private def fromApiIdentifier(id: value.Identifier): Either[String, Identifier] =
    for {
      packageId <- PackageId.fromString(id.packageId)
      moduleName <- DottedName.fromString(id.moduleName)
      entityName <- DottedName.fromString(id.entityName)
    } yield Identifier(packageId, QualifiedName(moduleName, entityName))
}
