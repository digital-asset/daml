// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure
import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  ExternalParty,
  LedgerTestSuite,
  LocalParty,
  Party,
}
import com.daml.ledger.api.testtool.suites.v2_1.CommandServiceIT.{
  createEventToDisclosedContract,
  formatByPartyAndTemplate,
}
import com.daml.ledger.api.testtool.suites.v2_1.CompanionImplicits.*
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.interactive.interactive_submission_service.Metadata.InputContract
import com.daml.ledger.api.v2.interactive.interactive_submission_service.Metadata.InputContract.Contract
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  CostEstimationHints,
  ExecuteSubmissionAndWaitForTransactionResponse,
  MinLedgerTime,
}
import com.daml.ledger.api.v2.package_reference.PackageReference
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.Command
import com.daml.ledger.test.java.model.test.{Dummy, DummyFactory}
import com.daml.ledger.test.java.semantic.divulgencetests.{
  DivulgenceProposal,
  DummyFlexibleController,
}
import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.InteractiveSubmissionExecuteError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidField
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound.PackageNamesNotFound
import com.digitalasset.canton.ledger.error.groups.{ConsistencyErrors, RequestValidationErrors}
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

final class InteractiveSubmissionServiceIT extends LedgerTestSuite with CommandSubmissionTestUtils {
  test(
    "ISSPrepareSubmissionRequestBasic",
    "Prepare a submission request",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val prepareRequest = ledger.prepareSubmissionRequest(
      party,
      new Dummy(party).create.commands,
    )
    for {
      response <- ledger.prepareSubmission(prepareRequest)
      tx = response.preparedTransaction
      hash = response.preparedTransactionHash
    } yield {
      assert(tx.nonEmpty, "The transaction was empty but shouldn't.")
      assert(response.costEstimation.nonEmpty, "Cost estimation should be defined by default")
      assert(!hash.isEmpty, "The hash was empty but shouldn't.")
    }
  })

  test(
    "ISSPrepareSubmissionRequestWithoutCostEstimation",
    "Prepare a submission request without cost estimation",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val prepareRequest = ledger.prepareSubmissionRequest(
      party,
      new Dummy(party).create.commands,
      estimateTrafficCost = Some(CostEstimationHints.defaultInstance.withDisabled(true)),
    )
    for {
      response <- ledger.prepareSubmission(prepareRequest)
      tx = response.preparedTransaction
      hash = response.preparedTransactionHash
    } yield {
      assert(tx.nonEmpty, "The transaction was empty but shouldn't.")
      assert(response.costEstimation.isEmpty, "Cost estimation should be empty")
      assert(!hash.isEmpty, "The hash was empty but shouldn't.")
    }
  })

  test(
    "ISSPrepareSubmissionRequestExplicitDisclosure",
    "Prepare a submission request with explicit disclosure",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner, stranger))) =>
    for {
      cidAndDisclosed <- testExplicitDisclosure(ledger, owner)
      (contractId, disclosedContract) = cidAndDisclosed
      prepareResponse <- ledger.prepareSubmission(
        ledger
          .prepareSubmissionRequest(stranger, contractId.exerciseFlexibleConsume(stranger).commands)
          .copy(disclosedContracts = Seq(disclosedContract))
      )
    } yield {
      assert(prepareResponse.preparedTransaction.nonEmpty, "prepared transaction was empty")
      val inputContractIds = for {
        tx <- prepareResponse.preparedTransaction
        metadata <- tx.metadata
      } yield metadata.inputContracts.map {
        case InputContract(Contract.V1(value), _createdAt, _eventBlob) => value.contractId
        case InputContract(Contract.Empty, _createdAt, _eventBlob) =>
          fail("Received empty input contract")
      }
      assert(inputContractIds.contains(Seq(contractId.contractId)), "Unexpected input contract id")
    }
  })

  test(
    "ISSPrepareSubmissionRequestFailOnUnknownContract",
    "Fail to prepare a submission request on an unknown contract",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner, stranger))) =>
    for {
      cidAndDisclosed <- testExplicitDisclosure(ledger, owner)
      (contractId, _) = cidAndDisclosed
      error <- ledger
        .prepareSubmission(
          ledger
            .prepareSubmissionRequest(
              stranger,
              contractId.exerciseFlexibleConsume(stranger).commands,
            )
        )
        .mustFail("missing input contract")
    } yield {
      assert(
        error.getMessage.contains("CONTRACT_NOT_FOUND"),
        s"wrong failure, got ${error.getMessage}",
      )
    }
  })

  test(
    "ISSPrepareSubmissionRequestSynchronizerId",
    "Prepare a submission request on a specific synchronizer",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    ledger
      .connectedSynchronizers()
      .flatMap { synchronizers =>
        Future.sequence {
          synchronizers.toList.map { synchronizerId =>
            val prepareRequest = ledger
              .prepareSubmissionRequest(
                party,
                new Dummy(party).create.commands,
              )
              .copy(
                synchronizerId = synchronizerId
              )
            for {
              response <- ledger.prepareSubmission(prepareRequest)
              tx = response.preparedTransaction
              hash = response.preparedTransactionHash
            } yield {
              assert(
                tx.value.metadata.value.synchronizerId == synchronizerId,
                "Unexpected synchronizer ID.",
              )
            }
          }
        }
      }
      .map(_ => ())
  })

  test(
    "ISSPrepareSubmissionRequestMinLedgerTime",
    "Prepare a submission request and respect min ledger time",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val time = Instant.now().plusSeconds(20)
    val prepareRequest = ledger
      .prepareSubmissionRequest(
        party,
        new Dummy(party)
          .createAnd()
          .exerciseConsumeIfTimeIsBetween(time, time.plusSeconds(1))
          .commands,
      )
      .copy(
        minLedgerTime = Some(MinLedgerTime(MinLedgerTime.Time.MinLedgerTimeAbs(Timestamp(time))))
      )
    for {
      response <- ledger.prepareSubmission(prepareRequest)
      tx = response.preparedTransaction
    } yield {
      val minLet = tx.value.metadata.value.minLedgerEffectiveTime
      val expected = LfTimestamp.assertFromInstant(time).micros
      assert(
        minLet.contains(expected),
        s"Incorrect min ledger time. Received: $minLet, expected: $expected",
      )
    }
  })

  test(
    "ISSPrepareSubmissionExecuteBasic",
    "Execute an externally signed transaction",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(externalParty: ExternalParty))) =>
    val prepareSubmissionRequest = ledger.prepareSubmissionRequest(
      externalParty,
      new Dummy(externalParty).create.commands,
    )
    for {
      prepareResponse <- ledger.prepareSubmission(prepareSubmissionRequest)
      executeRequest = ledger.executeSubmissionRequest(externalParty, prepareResponse)
      _ <- ledger.executeSubmission(executeRequest)
      _ <- ledger.firstCompletions(externalParty)
      transactions <- ledger.transactions(LedgerEffects, externalParty)
    } yield {
      val transaction = assertSingleton("expected one transaction", transactions)
      val event = transaction.events.headOption.value.event
      assert(event.isCreated)
      assert(transaction.externalTransactionHash.contains(prepareResponse.preparedTransactionHash))
    }
  })

  test(
    "ISSExecuteSubmissionAndWaitBasic",
    "Execute and wait for an externally signed transaction",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(externalParty: ExternalParty))) =>
    val prepareSubmissionRequest = ledger.prepareSubmissionRequest(
      externalParty,
      new Dummy(externalParty).create.commands,
    )
    for {
      prepareResponse <- ledger.prepareSubmission(prepareSubmissionRequest)
      executeAndWaitRequest = ledger.executeSubmissionAndWaitRequest(externalParty, prepareResponse)
      response <- ledger.executeSubmissionAndWait(executeAndWaitRequest)
      retrievedTransaction <- ledger.transactionById(response.updateId, Seq(externalParty))
    } yield {
      assert(
        response.updateId == retrievedTransaction.updateId,
        "ExecuteAndWait does not contain the expected updateId",
      )
      assert(
        response.completionOffset == retrievedTransaction.offset,
        "ExecuteAndWait does not contain the expected completion offset",
      )
      val event = retrievedTransaction.events.headOption.value.event
      assert(event.isCreated, "Expected created event")
      assert(
        retrievedTransaction.externalTransactionHash.contains(
          prepareResponse.preparedTransactionHash
        ),
        "Transaction hash was not set or incorrect",
      )
    }
  })

  test(
    "ISSPrepareSubmissionFailExecuteOnInvalidSignature",
    "Fail execute if the signature is invalid",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(externalParty: ExternalParty))) =>
    val prepareSubmissionRequest = ledger.prepareSubmissionRequest(
      externalParty,
      new Dummy(externalParty).create.commands,
    )
    for {
      prepareResponse <- ledger.prepareSubmission(prepareSubmissionRequest)
      executeRequest = ledger
        .executeSubmissionRequest(externalParty, prepareResponse)
        // Mess with the signature
        .update(
          _.partySignatures.signatures.modify(
            _.map(
              _.update(_.signatures.modify(_.map(_.update(_.signature.modify(_.substring(1))))))
            )
          )
        )
      _ <- ledger
        .executeSubmission(executeRequest)
        .mustFailWith(
          "Invalid signature",
          InteractiveSubmissionExecuteError.code,
          Some("Received 0 valid signatures from distinct keys (1 invalid)"),
        )
    } yield ()
  })

  test(
    "ISSPrepareSubmissionFailExecuteAndWaitOnInvalidSignature",
    "Fail execute and wait if the signature is invalid",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(externalParty: ExternalParty))) =>
    val prepareSubmissionRequest = ledger.prepareSubmissionRequest(
      externalParty,
      new Dummy(externalParty).create.commands,
    )
    for {
      prepareResponse <- ledger.prepareSubmission(prepareSubmissionRequest)
      executeAndWaitRequest = ledger
        .executeSubmissionAndWaitRequest(externalParty, prepareResponse)
        .update(
          _.partySignatures.signatures.modify(
            _.map(
              _.update(_.signatures.modify(_.map(_.update(_.signature.modify(_.substring(1))))))
            )
          )
        )
      _ <- ledger
        .executeSubmissionAndWait(executeAndWaitRequest)
        .mustFailWith(
          "Invalid signature",
          InteractiveSubmissionExecuteError.code,
          Some("Received 0 valid signatures from distinct keys (1 invalid)"),
        )
    } yield ()
  })

  test(
    "ISSPrepareSubmissionFailExecuteOnInvalidSignatory",
    "Fail execute if signed by a non signatory party",
    allocate(TwoExternalParties),
  )(implicit ec => {
    case Participants(Participant(ledger, Seq(alice: ExternalParty, bob: ExternalParty))) =>
      val prepareSubmissionRequest = ledger.prepareSubmissionRequest(
        alice,
        new Dummy(alice).create.commands,
      )
      for {
        prepareResponse <- ledger.prepareSubmission(prepareSubmissionRequest)
        // bob signs instead of alice
        executeRequest = ledger.executeSubmissionRequest(bob, prepareResponse)
        _ <- ledger
          .executeSubmission(executeRequest)
          .mustFailWith(
            "Missing signature",
            InteractiveSubmissionExecuteError.code,
            Some("The following actAs parties did not provide an external signature"),
          )
      } yield ()
  })

  test(
    "ISSExecuteSubmissionRequestWithInputContracts",
    "Submit with input contracts",
    allocate(TwoExternalParties),
  )(implicit ec => {
    case Participants(Participant(ledger, Seq(owner: ExternalParty, stranger: ExternalParty))) =>
      for {
        cidAndDisclosed <- testExplicitDisclosure(ledger, owner)
        (contractId, disclosedContract) = cidAndDisclosed
        prepareResponse <- ledger.prepareSubmission(
          ledger
            .prepareSubmissionRequest(
              stranger,
              contractId.exerciseFlexibleConsume(stranger).commands,
            )
            .copy(disclosedContracts = Seq(disclosedContract))
        )
        executeRequest = ledger.executeSubmissionRequest(stranger, prepareResponse)
        _ <- ledger.executeSubmission(executeRequest)
        _ <- ledger.firstCompletions(stranger)
        transactions <- ledger.transactions(LedgerEffects, stranger)
      } yield {
        val transaction = transactions.headOption.value
        val event = transaction.events.headOption.value.event
        assert(event.isExercised)
        assert(
          transaction.externalTransactionHash.contains(prepareResponse.preparedTransactionHash)
        )
      }
  })

  test(
    "ISSExecuteSubmissionRequestFailOnEmptyInputContracts",
    "Fail if input contracts are missing",
    allocate(TwoExternalParties),
  )(implicit ec => {
    case Participants(Participant(ledger, Seq(owner: ExternalParty, stranger: ExternalParty))) =>
      for {
        cidAndDisclosed <- testExplicitDisclosure(ledger, owner)
        (contractId, disclosedContract) = cidAndDisclosed
        prepareResponse <- ledger.prepareSubmission(
          ledger
            .prepareSubmissionRequest(
              stranger,
              contractId.exerciseFlexibleConsume(stranger).commands,
            )
            .copy(disclosedContracts = Seq(disclosedContract))
        )
        executeRequest = ledger
          .executeSubmissionRequest(stranger, prepareResponse)
          // Remove input contracts
          .update(_.preparedTransaction.metadata.inputContracts.set(Seq.empty))
        _ <- ledger
          .executeSubmission(executeRequest)
          .mustFailWith(
            "Missing input contracts",
            InteractiveSubmissionExecuteError.code,
            Some("Missing input contracts"),
          )
      } yield ()
  })

  test(
    "ISSDuplicateExecuteAndWaitForTransactionData",
    "ExecuteSubmissionAndWaitForTransaction should fail on duplicate requests",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party: ExternalParty))) =>
    val prepareRequest =
      ledger.prepareSubmissionRequest(party, new Dummy(party).create.commands)
    for {
      prepareResponse <- ledger.prepareSubmission(prepareRequest)
      executeRequest = ledger.executeSubmissionAndWaitForTransactionRequest(
        party,
        prepareResponse,
        None,
      )
      _ <- ledger.executeSubmissionAndWaitForTransaction(executeRequest)
      failure <- ledger
        .submitRequestAndTolerateGrpcError(
          ConsistencyErrors.SubmissionAlreadyInFlight,
          _.executeSubmissionAndWaitForTransaction(executeRequest),
        )
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.DuplicateCommand,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ISSDuplicateExecuteAndWaitData",
    "ExecuteSubmissionAndWait should fail on duplicate requests",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party: ExternalParty))) =>
    val prepareRequest =
      ledger.prepareSubmissionRequest(party, new Dummy(party).create.commands)
    for {
      prepareResponse <- ledger.prepareSubmission(prepareRequest)
      executeRequest = ledger.executeSubmissionAndWaitRequest(
        party,
        prepareResponse,
      )
      _ <- ledger.executeSubmissionAndWait(executeRequest)
      failure <- ledger
        .submitRequestAndTolerateGrpcError(
          ConsistencyErrors.SubmissionAlreadyInFlight,
          _.executeSubmissionAndWait(executeRequest),
        )
        .mustFail("submitting a duplicate request")
    } yield {
      assertGrpcError(
        failure,
        ConsistencyErrors.DuplicateCommand,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "ISSExecuteSubmissionAndWaitForTransactionFilterByTemplateId",
    "ExecuteSubmissionAndWaitForTransaction returns a filtered transaction (by template id)",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val format = ledger.transactionFormat(
      parties = Some(Seq(party)),
      transactionShape = AcsDelta,
    )
    for {
      createdTransaction <- executeAndWaitForTransaction(
        ledger,
        party,
        new DummyFactory(party).create.commands,
        Some(format),
      )
      transaction = assertDefined(
        createdTransaction.transaction,
        "The transaction should be defined",
      )
      exerciseTransactionO <- executeAndWaitForTransaction(
        ledger,
        party,
        DummyFactory.ContractId
          .fromContractId(
            new com.daml.ledger.javaapi.data.codegen.ContractId(
              transaction.events.headOption.value.getCreated.contractId
            )
          )
          .exerciseDummyFactoryCall()
          .commands(),
        Some(
          ledger.transactionFormat(
            parties = Some(Seq(party)),
            transactionShape = AcsDelta,
            templateIds = Seq(Dummy.TEMPLATE_ID),
          )
        ),
      )
      exerciseTransaction = assertDefined(
        exerciseTransactionO.transaction,
        "The transaction should be defined",
      )
    } yield {
      // The transaction creates 2 contracts of type Dummy and DummyWithParam
      // But because we filter by Dummy template we should only get that one
      assertLength(
        "Two create event should have been into the transaction",
        1,
        exerciseTransaction.events,
      )
      val templateId = assertDefined(
        assertSingleton("expected single event", exerciseTransaction.events).getCreated.templateId,
        "expected template id",
      )
      assert(templateId.packageId == Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.getPackageId)
      assert(templateId.moduleName == Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.getModuleName)
      assert(templateId.entityName == Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.getEntityName)
    }
  })

  test(
    "ISSExecuteAndWaitForTransactionBasic",
    "ExecuteSubmissionAndWaitForTransaction returns a transaction",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      transactionResponse <- executeAndWaitForTransaction(
        ledger,
        party,
        new Dummy(party).create.commands,
        Some(
          ledger.transactionFormat(
            parties = Some(Seq(party))
          )
        ),
      )
    } yield {
      assertOnTransactionResponse(transactionResponse.getTransaction)
    }
  })

  test(
    "ISSExecuteAndWaitForTransactionNoFilter",
    "ExecuteSubmissionAndWaitForTransaction returns a transaction with no filter",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      transactionResponse <- executeAndWaitForTransaction(
        ledger,
        party,
        new Dummy(party).create.commands,
        None,
      )
    } yield {
      assertOnTransactionResponse(transactionResponse.getTransaction)
    }
  })

  test(
    "ISSExecuteAndWaitForTransactionFilterByWrongParty",
    "ExecuteSubmissionAndWaitForTransaction returns a transaction with empty events when filtered by wrong party",
    allocate(TwoExternalParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, party2))) =>
    val format = ledger.transactionFormat(
      parties = Some(Seq(party2)),
      transactionShape = AcsDelta,
    )
    for {
      transactionResponseAcsDelta <- executeAndWaitForTransaction(
        ledger,
        party,
        new Dummy(party).create.commands,
        Some(format),
      )
      transactionResponseLedgerEffects <- executeAndWaitForTransaction(
        ledger,
        party,
        new Dummy(party).create.commands,
        Some(format.update(_.transactionShape := TRANSACTION_SHAPE_LEDGER_EFFECTS)),
      )
    } yield {
      assertLength(
        "No events should have been into the transaction",
        0,
        transactionResponseAcsDelta.transaction.toList.flatMap(_.events),
      )
      assertLength(
        "No events should have been into the transaction",
        0,
        transactionResponseLedgerEffects.transaction.toList.flatMap(_.events),
      )
    }
  })

  test(
    "ISSExecuteAndWaitInvalidSynchronizerId",
    "ExecuteSubmissionAndWait should fail for invalid synchronizer ids",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party: ExternalParty))) =>
    val invalidSynchronizerId = "invalidSynchronizerId"
    val prepareRequest = ledger.prepareSubmissionRequest(
      party,
      new Dummy(party).create.commands,
    )
    for {
      prepareResponse <- ledger.prepareSubmission(prepareRequest)
      executeAndWaitRequest = ledger
        .executeSubmissionAndWaitRequest(
          party,
          prepareResponse,
        )
        .update(_.preparedTransaction.metadata.synchronizerId := invalidSynchronizerId)
      failure <- ledger
        .executeSubmissionAndWait(executeAndWaitRequest)
        .mustFail(
          "submitting a request with an invalid synchronizer id"
        )
    } yield assertGrpcError(
      failure,
      RequestValidationErrors.InvalidField,
      Some(
        s"Invalid field synchronizer_id: Invalid unique identifier `$invalidSynchronizerId` with missing namespace."
      ),
      checkDefiniteAnswerMetadata = true,
    )
  })

  test(
    "ISSExecuteAndWaitForTransactionInvalidSynchronizerId",
    "ExecuteSubmissionAndWaitForTransaction should fail for invalid synchronizer ids",
    allocate(SingleExternalParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party: ExternalParty))) =>
    val invalidSynchronizerId = "invalidSynchronizerId"
    val prepareRequest = ledger.prepareSubmissionRequest(
      party,
      new Dummy(party).create.commands,
    )
    for {
      prepareResponse <- ledger.prepareSubmission(prepareRequest)
      executeAndWaitRequest = ledger
        .executeSubmissionAndWaitForTransactionRequest(
          party,
          prepareResponse,
          None,
        )
        .update(_.preparedTransaction.metadata.synchronizerId := invalidSynchronizerId)
      failure <- ledger
        .executeSubmissionAndWaitForTransaction(executeAndWaitRequest)
        .mustFail(
          "submitting a request with an invalid synchronizer id"
        )
    } yield assertGrpcError(
      failure,
      RequestValidationErrors.InvalidField,
      Some(
        s"Invalid field synchronizer_id: Invalid unique identifier `$invalidSynchronizerId` with missing namespace."
      ),
      checkDefiniteAnswerMetadata = true,
    )
  })

  test(
    "ISSPreferredPackagesKnown",
    "Getting preferred packages should return a valid result",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    for {
      result <- ledger.getPreferredPackages(
        Map(
          Dummy.PACKAGE_NAME -> Seq(party1, party2),
          DivulgenceProposal.PACKAGE_NAME -> Seq(party1, party2),
        )
      )
    } yield assertSameElements(
      result.packageReferences.sortBy(_.packageId),
      Seq(
        PackageReference(
          packageId = Dummy.PACKAGE_ID,
          packageName = Dummy.PACKAGE_NAME,
          packageVersion = Dummy.PACKAGE_VERSION.toString,
        ),
        PackageReference(
          packageId = DivulgenceProposal.PACKAGE_ID,
          packageName = DivulgenceProposal.PACKAGE_NAME,
          packageVersion = DivulgenceProposal.PACKAGE_VERSION.toString,
        ),
      ),
    )
  })

  test(
    "ISSPreferredPackagesUnknownParty",
    "Getting preferred package version for an unknown party should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      invalidPartyFailure <- ledger
        .getPreferredPackages(
          // Manually craft invalid party
          Map(Dummy.PACKAGE_NAME -> Seq(Party(new data.Party("invalid-party"))))
        )
        .mustFail("invalid party")
      unknownPartyFailure <- ledger
        .getPreferredPackages(
          // Manually craft invalid party
          Map(Dummy.PACKAGE_NAME -> Seq(Party(new data.Party("unknownParty::ns"))))
        )
        .mustFail("unknown party")
    } yield {
      assertGrpcError(
        // TODO(#25385): Here we should first report the invalid party-id format
        invalidPartyFailure,
        UnknownInformees,
        None,
      )
      assertGrpcError(
        unknownPartyFailure,
        UnknownInformees,
        None,
      )
    }
  })

  test(
    "ISSPreferredPackagesUnknownPackageName",
    "Getting preferred package version for an unknown package-name should fail",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      invalidPackageNameFailure <- ledger
        .getPreferredPackages(Map("What-Is-A-Package-Name?" -> Seq(party)))
        .mustFail("invalid package-name")
      unknownPackageNameFailure <- ledger
        .getPreferredPackages(Map("NoSuchPackage" -> Seq(party)))
        .mustFail("unknown package-name")
    } yield {
      assertGrpcError(
        invalidPackageNameFailure,
        InvalidField,
        Some("package_name/packageName"),
      )
      assertGrpcError(
        unknownPackageNameFailure,
        PackageNamesNotFound,
        None,
      )
    }
  })

  test(
    "ISSPreferredPackagesUnknownSynchronizerId",
    "Getting preferred package version for an unknown synhcronizer-id should fail",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    for {
      invalidSynchronizerIdFailure <- ledger
        .getPreferredPackages(
          Map(Dummy.PACKAGE_NAME -> Seq(party1, party2)),
          synchronizerIdO = Some("invalidSyncId"),
        )
        .mustFail("unknown synchronizer-id")
      unknownSynchronizerIdFailure <- ledger
        .getPreferredPackages(
          Map(Dummy.PACKAGE_NAME -> Seq(party1, party2)),
          synchronizerIdO = Some("unknownSynchronizerId::ns"),
        )
        .mustFail("unknown synchronizer-id")
    } yield {
      assertGrpcError(
        invalidSynchronizerIdFailure,
        InvalidField,
        Some("synchronizer_id/synchronizerId"),
      )
      assertGrpcError(
        unknownSynchronizerIdFailure,
        InvalidPrescribedSynchronizerId,
        None,
      )
    }
  })

  test(
    "ISSPreferredPackageVersionKnown",
    "Getting preferred package version should return a valid result",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    for {
      result <- ledger.getPreferredPackageVersion(Seq(party1, party2), Dummy.PACKAGE_NAME)
    } yield {
      result.packagePreference
        .flatMap(_.packageReference)
        .map(
          assertEquals(
            _,
            PackageReference(
              packageId = Dummy.PACKAGE_ID,
              packageName = Dummy.PACKAGE_NAME,
              packageVersion = Dummy.PACKAGE_VERSION.toString,
            ),
          )
        )
        .getOrElse(fail(s"Invalid preference response: $result"))
    }
  })

  test(
    "ISSPreferredPackageVersionUnknownParty",
    "Getting preferred package version for an unknown party should fail",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      invalidPartyFailure <- ledger
        .getPreferredPackageVersion(
          // Manually craft invalid party
          Seq(Party(new data.Party("invalid-party"))),
          Dummy.PACKAGE_NAME,
        )
        .mustFail("invalid party")
      unknownPartyFailure <- ledger
        .getPreferredPackageVersion(
          // Manually craft invalid party
          Seq(Party(new data.Party("unknownParty::ns"))),
          Dummy.PACKAGE_NAME,
        )
        .mustFail("unknown party")
    } yield {
      assertGrpcError(
        // TODO(#25385): Here we should first report the invalid party-id format
        invalidPartyFailure,
        UnknownInformees,
        None,
      )
      assertGrpcError(
        unknownPartyFailure,
        UnknownInformees,
        None,
      )
    }
  })

  test(
    "ISSPreferredPackageVersionUnknownPackageName",
    "Getting preferred package version for an unknown package-name should fail",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      invalidPackageNameFailure <- ledger
        .getPreferredPackageVersion(Seq(party), "What-Is-A-Package-Name?")
        .mustFail("invalid package-name")
      unknownPackageNameFailure <- ledger
        .getPreferredPackageVersion(Seq(party), "NoSuchPackage")
        .mustFail("unknown package-name")
    } yield {
      assertGrpcError(
        invalidPackageNameFailure,
        InvalidField,
        Some("package_name/packageName"),
      )
      assertGrpcError(
        unknownPackageNameFailure,
        PackageNamesNotFound,
        None,
      )
    }
  })

  test(
    "ISSPreferredPackageVersionUnknownSynchronizerId",
    "Getting preferred package version for an unknown synhcronizer-id should fail",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    for {
      invalidSynchronizerIdFailure <- ledger
        .getPreferredPackageVersion(
          Seq(party1, party2),
          Dummy.PACKAGE_NAME,
          synchronizerIdO = Some("invalidSyncId"),
        )
        .mustFail("unknown synchronizer-id")
      unknownSynchronizerIdFailure <- ledger
        .getPreferredPackageVersion(
          Seq(party1, party2),
          Dummy.PACKAGE_NAME,
          synchronizerIdO = Some("unknownSynchronizerId::ns"),
        )
        .mustFail("unknown synchronizer-id")
    } yield {
      assertGrpcError(
        invalidSynchronizerIdFailure,
        InvalidField,
        Some("synchronizer_id/synchronizerId"),
      )
      assertGrpcError(
        unknownSynchronizerIdFailure,
        InvalidPrescribedSynchronizerId,
        None,
      )
    }
  })

  private def testExplicitDisclosure(
      ledger: ParticipantTestContext,
      owner: Party,
  )(implicit
      ec: ExecutionContext
  ): Future[(DummyFlexibleController.ContractId, DisclosedContract)] = {
    def create: Future[DummyFlexibleController.ContractId] = owner match {
      case local: LocalParty =>
        ledger.create(
          owner,
          new DummyFlexibleController(owner),
        )
      case external: infrastructure.ExternalParty =>
        val prepareSubmissionRequest = ledger.prepareSubmissionRequest(
          external,
          new DummyFlexibleController(external).create.commands,
        )
        for {
          prepareResponse <- ledger.prepareSubmission(prepareSubmissionRequest)
          executeRequest = ledger.executeSubmissionRequest(external, prepareResponse)
          _ <- ledger.executeSubmission(executeRequest)
          _ <- ledger.firstCompletions(external)
          transactions <- ledger.transactions(LedgerEffects, external)
        } yield {
          DummyFlexibleController.ContractId.fromContractId(
            new com.daml.ledger.javaapi.data.codegen.ContractId(
              transactions.headOption.value.events.headOption.value.getCreated.contractId
            )
          )
        }
    }

    for {
      contractId: DummyFlexibleController.ContractId <- create
      end <- ledger.currentEnd()
      witnessTxs <- ledger.transactions(
        new GetUpdatesRequest(
          beginExclusive = ledger.begin,
          endInclusive = Some(end),
          updateFormat = Some(formatByPartyAndTemplate(owner, DummyFlexibleController.TEMPLATE_ID)),
        )
      )
      tx = assertSingleton("Owners' transactions", witnessTxs)
      create = assertSingleton("The create", createdEvents(tx))
      disclosedContract = createEventToDisclosedContract(create)
    } yield (contractId, disclosedContract)
  }

  def executeAndWaitForTransaction(
      ledger: ParticipantTestContext,
      party: Party,
      commands: java.util.List[Command],
      transactionFormat: Option[com.daml.ledger.api.v2.transaction_filter.TransactionFormat] = None,
  )(implicit
      executionContext: ExecutionContext
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
    party match {
      case externalParty: ExternalParty =>
        val prepareRequest = ledger.prepareSubmissionRequest(
          externalParty,
          commands,
        )
        for {
          prepareResponse <- ledger.prepareSubmission(prepareRequest)
          executeAndWaitRequest = ledger.executeSubmissionAndWaitForTransactionRequest(
            externalParty,
            prepareResponse,
            transactionFormat,
          )
          response <- ledger.executeSubmissionAndWaitForTransaction(executeAndWaitRequest)
        } yield response
      case _ => fail("Expected an external party")
    }

  // TODO(#25385): Add test for GetPreferredPackages.vettingValidAt
}
