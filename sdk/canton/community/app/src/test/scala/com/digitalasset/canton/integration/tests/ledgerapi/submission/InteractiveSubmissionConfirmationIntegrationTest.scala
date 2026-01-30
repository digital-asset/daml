// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.interactive.interactive_submission_service.PrepareSubmissionResponse
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.crypto.{
  InteractiveSubmission,
  Signature,
  SigningKeyUsage,
  SigningKeysWithThreshold,
}
import com.digitalasset.canton.damltests.java.statictimetest.Pass
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LogEntry,
  LoggingContextWithTrace,
  SuppressionRule,
}
import com.digitalasset.canton.platform.apiserver.execution.CommandInterpretationResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.PreparedTransactionDecoder
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.MalformedRequest
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.sequencing.protocol.MemberRecipient
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.canton.{HasExecutionContext, LfTimestamp}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{SubmissionId, UserId}
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.Seq
import scala.concurrent.{Await, Future, Promise}
import scala.util.Failure

/** Interactive submission test that asserts the behavior in phase 3 (confirmation)
  */
final class InteractiveSubmissionConfirmationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasProgrammableSequencer
    with HasExecutionContext
    with HasCycleUtils {

  private var aliceE: ExternalParty = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)

        aliceE = cpn.parties.testing.external.enable(
          "Alice",
          // Use 3 keys but start with a threshold of 1
          keysCount = PositiveInt.three,
          keysThreshold = PositiveInt.one,
        )
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
      .addConfigTransform(
        ConfigTransforms
          .updateAllParticipantConfigs_(
            // Disable in this suite so we can perform phase 3 assertions
            _.focus(_.ledgerApi.interactiveSubmissionService.enforceSingleRootNode)
              .replace(false)
          )
      )

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def cpn(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    env.participant1

  "Interactive submission in phase 3" should {
    val malformedRequestLogAssertion: Seq[(LogEntry => Assertion, String)] = Seq(
      (
        _.warningMessage should (include regex s"${MalformedRequest.id}.*with a view that is not correctly authenticated"),
        "Expected malformed request log",
      )
    )

    def invalidSignaturesLogAssertion(
        valid: Int,
        invalid: Int,
        expectedValid: Int,
    ): Seq[(LogEntry => Assertion, String)] = Seq(
      (
        {
          _.warningMessage should include(
            s"Received $valid valid signatures from distinct keys ($invalid invalid), but expected at least $expectedValid valid for ${aliceE.partyId}"
          )
        },
        "Expected invalid signature log",
      )
    )

    def bypassPhase1Validations(
        assertion: (
            PrepareSubmissionResponse,
            Map[PartyId, Seq[Signature]],
            Promise[Unit],
            AtomicBoolean,
        ) => Assertion,
        expectedLogs: Seq[(LogEntry => Assertion, String)],
        signaturesModifier: Map[PartyId, Seq[Signature]] => Map[PartyId, Seq[Signature]] = identity,
        externalParty: ExternalParty = aliceE,
        expectMalformedRequest: Boolean = true,
    )(implicit env: FixtureParam): Unit = {
      import env.*

      val sequencer = getProgrammableSequencer(env.sequencer1.name)
      val hasReachedSequencer = new AtomicBoolean(false)

      val prepared = cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(externalParty.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            externalParty.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )
      val signatures = signaturesModifier(
        Map(
          externalParty.partyId -> global_secret.sign(
            prepared.preparedTransactionHash,
            externalParty,
            useAllKeys = true,
          )
        )
      )

      // To bypass the checks in phase 1 we play a trick by holding back the submission request in the sequencer
      // while we change the signing keys, and release afterwards
      val releaseSubmission = Promise[Unit]()
      sequencer.setPolicy_("hold submission") {
        case submission if submission.batch.envelopes.exists(_.recipients.allRecipients.exists {
              case MemberRecipient(member) =>
                member == cpn.id
              case _ =>
                false
            }) =>
          hasReachedSequencer.set(true)
          SendDecision.HoldBack(releaseSubmission.future)
        case _ => SendDecision.Process
      }
      loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.WARN))(
        assertion(prepared, signatures, releaseSubmission, hasReachedSequencer),
        LogEntry.assertLogSeq(
          Option
            .when(expectMalformedRequest)(malformedRequestLogAssertion)
            .getOrElse(Seq.empty) ++ expectedLogs,
          Seq.empty,
        ),
      )
    }

    "fail if the number of signatures is under the threshold" in { implicit env =>
      bypassPhase1Validations(
        { (prepared, signatures, releaseSubmission, _) =>
          val (submissionId, ledgerEnd) = exec(prepared, signatures, cpn)

          // Change the threshold
          aliceE.topology.party_to_participant_mappings
            .propose_delta(
              cpn,
              store = env.daId,
              newSigningThreshold = Some(PositiveInt.two),
            )

          // Update alice with the new threshold for subsequent tests
          aliceE = aliceE.copy(signingThreshold = PositiveInt.two)
          releaseSubmission.success(())
          val completion = findCompletion(
            submissionId,
            ledgerEnd,
            aliceE,
            cpn,
          )
          // Transaction should fail
          completion.status.value.code shouldBe Status.Code.INVALID_ARGUMENT.value()
        },
        signaturesModifier = _.updatedWith(aliceE.partyId)(_.map(_.take(1))),
        expectedLogs = invalidSignaturesLogAssertion(valid = 1, invalid = 0, expectedValid = 2),
      )
    }

    "fail if the signatures are invalid" in { implicit env =>
      bypassPhase1Validations(
        { (prepared, signatures, releaseSubmission, _) =>
          val (submissionId, ledgerEnd) = exec(prepared, signatures, cpn)

          val newKeys = NonEmpty.mk(
            Seq,
            env.global_secret.get_signing_key(aliceE.fingerprint),
            env.global_secret.keys.secret.generate_key(usage = SigningKeyUsage.ProtocolOnly),
            env.global_secret.keys.secret.generate_key(usage = SigningKeyUsage.ProtocolOnly),
          )

          // Change the protocol keys and threshold
          aliceE.topology.party_to_participant_mappings
            .propose(
              cpn,
              Seq(cpn.id -> ParticipantPermission.Confirmation),
              store = env.daId,
              partySigningKeys = Some(
                SigningKeysWithThreshold.tryCreate(
                  newKeys,
                  PositiveInt.two,
                )
              ),
            )

          // Update alice with the new keys for subsequent tests
          aliceE = aliceE.copy(signingFingerprints = newKeys.map(_.fingerprint).toSeq)
          releaseSubmission.success(())
          val completion = findCompletion(
            submissionId,
            ledgerEnd,
            aliceE,
            cpn,
          )
          // Transaction should fail
          completion.status.value.code shouldBe Status.Code.INVALID_ARGUMENT.value()
        },
        // One is valid (the party namespace one, which has not changed)
        expectedLogs = invalidSignaturesLogAssertion(valid = 1, invalid = 2, expectedValid = 2),
      )
    }

    "fail execute and wait if the signatures are invalid" in { implicit env =>
      bypassPhase1Validations(
        { (prepared, signatures, releaseSubmission, _) =>
          val response = Future(execAndWait(prepared, signatures, execParticipant = _ => cpn))

          val newKeys = NonEmpty.mk(
            Seq,
            env.global_secret.get_signing_key(aliceE.fingerprint),
            env.global_secret.keys.secret
              .generate_keys(PositiveInt.three, usage = SigningKeyUsage.ProtocolOnly)*
          )

          // Change the protocol keys
          aliceE.topology.party_to_participant_mappings
            .propose(
              cpn,
              Seq(cpn.id -> ParticipantPermission.Confirmation),
              store = env.daId,
              partySigningKeys = Some(
                SigningKeysWithThreshold.tryCreate(
                  newKeys,
                  PositiveInt.two,
                )
              ),
            )

          // Update alice with the new keys for subsequent tests
          aliceE = aliceE.copy(signingFingerprints = newKeys.map(_.fingerprint))
          releaseSubmission.success(())
          val res = Await.ready(response, timeouts.default.duration)
          res.value match {
            case Some(Failure(_: CommandFailure)) => succeed
            case _ => fail("Expected a command failure")
          }
        },
        expectedLogs =
          // One is valid (the party namespace one, which has not changed)
          invalidSignaturesLogAssertion(valid = 1, invalid = 2, expectedValid = 2) ++ Seq(
            (
              _.errorMessage should include(
                s"Request failed for ${cpn.name}"
              ),
              "expect invalid signatures",
            )
          ),
      )
    }

    "fail if there is an externally signed tx with more than a single view" in { implicit env =>
      import env.*

      // Use create-and-execute to create a multi view request
      val pass =
        new Pass("create-and-exe", aliceE.toProtoPrimitive, env.environment.clock.now.toInstant)
          .createAnd()
          .exerciseGetTime()
      val prepared = cpn.ledger_api.javaapi.interactive_submission
        .prepare(Seq(aliceE.partyId), Seq(pass.commands().loneElement))
      val signatures = Map(
        aliceE.partyId -> global_secret.sign(
          prepared.preparedTransactionHash,
          aliceE,
          useAllKeys = true,
        )
      )
      val completion =
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            val (submissionId, ledgerEnd) = exec(prepared, signatures, cpn)
            findCompletion(submissionId, ledgerEnd, aliceE, cpn)
          },
          LogEntry.assertLogSeq(malformedRequestLogAssertion),
        )
      completion.status.value.code shouldBe Status.Code.INVALID_ARGUMENT.value()
    }

    "fail with missing input contracts" in { implicit env =>
      import env.*
      import monocle.syntax.all.*

      // Set Alice back to threshold one
      aliceE.topology.party_to_participant_mappings
        .propose_delta(
          cpn,
          store = env.daId,
          newSigningThreshold = Some(PositiveInt.one),
        )

      // Exercise the Repeat choice
      val exerciseRepeatOnCycleContract =
        createCycleContract(cpn, aliceE, "test-external-signing").id
          .exerciseRepeat()
          .commands()
          .loneElement

      val prepared = cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(exerciseRepeatOnCycleContract),
      )

      val decoder = new PreparedTransactionDecoder(loggerFactory)
      val deserialized: CommandInterpretationResult = decoder
        .decode(
          ExecuteRequest(
            UserId.assertFromString("app"),
            SubmissionId.assertFromString(UUID.randomUUID().toString),
            DeduplicationOffset(None),
            Map.empty,
            prepared.getPreparedTransaction,
            HashingSchemeVersion.V2,
            sequencer1.synchronizer_id,
            tentativeLedgerEffectiveTime = LfTimestamp.now(),
          )
        )(executionContext, LoggingContextWithTrace.ForTesting, implicitly[ErrorLoggingContext])
        .futureValue
        .impoverish
        // We remove the input contracts from the deserialized tx
        .focus(_.processedDisclosedContracts)
        .replace(ImmArray.empty)

      // Recompute the hash on the transaction without the input contracts
      val reComputedHashWithMissingInputContract = InteractiveSubmission
        .computeVersionedHash(
          HashingSchemeVersion.V2,
          deserialized.transaction,
          TransactionMetadataForHashing.create(
            deserialized.submitterInfo.actAs.toSet,
            deserialized.submitterInfo.commandId,
            deserialized.submitterInfo.externallySignedSubmission.value.transactionUUID,
            deserialized.submitterInfo.externallySignedSubmission.value.mediatorGroup.value,
            sequencer1.synchronizer_id,
            deserialized.transactionMeta.timeBoundaries,
            deserialized.transactionMeta.preparationTime,
            maxRecordTime = None,
            deserialized.processedDisclosedContracts.map(fci => fci.contractId -> fci).toList.toMap,
          ),
          deserialized.transactionMeta.optNodeSeeds.value.toSeq.toMap,
          testedProtocolVersion,
          HashTracer.NoOp,
        )
        .value

      // Sign it
      val signature = env.global_secret.sign(
        reComputedHashWithMissingInputContract.unwrap,
        // In this test we assume alice has only one signing key
        aliceE.signingFingerprints.head1,
        SigningKeyUsage.ProtocolOnly,
      )

      // Replace the externally signed signature in the submitter info with the new one
      // This makes the signature valid with respect to the empty input contract submission, and will
      // pass the signature check during phase 1
      val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
      val submitterInfo = deserialized.submitterInfo.copy(
        externallySignedSubmission = Some(
          deserialized.submitterInfo.externallySignedSubmission.value.copy(
            signatures = Map(aliceE.partyId -> Seq(signature))
          )
        ),
        submissionId = Some(submissionId),
      )

      loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.WARN))(
        {
          val participant = cpn.runningNode.value.getNode.value
          val routingSynchronizerState = participant.sync.getRoutingSynchronizerState.futureValueUS
          val synchronizerRank = participant.sync
            .selectRoutingSynchronizer(
              submitterInfo,
              deserialized.transaction,
              deserialized.transactionMeta,
              List.empty,
              Some(sequencer1.synchronizer_id),
              transactionUsedForExternalSigning = true,
              routingSynchronizerState,
            )
            .futureValueUS
            .value

          // Submit the tx directly to the participant sync service
          // This bypasses the check in the LAPI that the input contracts of the transaction are all explicitly provided
          // in the execute request (which would fail otherwise)
          participant.sync
            .submitTransaction(
              deserialized.transaction,
              synchronizerRank,
              routingSynchronizerState,
              submitterInfo,
              deserialized.transactionMeta,
              _estimatedInterpretationCost = 0L,
              deserialized.globalKeyMapping,
              ImmArray.empty,
            )
            .futureValue
        },
        LogEntry.assertLogSeq(
          malformedRequestLogAssertion ++ invalidSignaturesLogAssertion(
            valid = 0,
            invalid = 1,
            expectedValid = 1,
          )
        ),
      )
    }
  }
}
