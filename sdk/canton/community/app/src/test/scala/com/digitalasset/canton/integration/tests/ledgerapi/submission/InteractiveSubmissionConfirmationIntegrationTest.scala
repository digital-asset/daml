// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.interactive.interactive_submission_service.PrepareSubmissionResponse
import com.daml.nonempty.NonEmptyUtil
import com.daml.scalautil.future.FutureConversion.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.crypto.{InteractiveSubmission, Signature, SigningKeyUsage}
import com.digitalasset.canton.damltests.java.statictimetest.Pass
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.defaultConfirmingParticipant
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
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
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.sequencing.protocol.MemberRecipient
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.canton.{HasExecutionContext, LfTimestamp}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{SubmissionId, UserId}
import io.grpc.Status
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.UUID
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
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)

        aliceE = cpn.parties.external.enable(
          "Alice",
          // Use 3 keys but start with a threshold of 1
          keysCount = PositiveInt.three,
          keysThreshold = PositiveInt.one,
        )
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  "Interactive submission in phase 3" should {
    "fail if the number of signatures is under the threshold" in { implicit env =>
      val sequencer = getProgrammableSequencer(env.sequencer1.name)

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      // Sign with a single key
      val singleSignature = env.global_secret.sign(
        prepared.preparedTransactionHash,
        aliceE.signingFingerprints.head1,
        SigningKeyUsage.ProtocolOnly,
      )

      val localVerdictWarning = Seq(2, 3).map[(LogEntry => Assertion, String)]({ p =>
        (
          e => {
            e.warningMessage should (include regex "LOCAL_VERDICT_MALFORMED_REQUEST.*with a view that is not correctly authenticated")
            e.mdc.get("participant") shouldBe Some(s"participant$p")
          },
          s"participant$p authentication",
        )
      })

      // To bypass the checks in phase 1 we play a trick by holding back the submission request in the sequencer
      // while we increase the key threshold, and release afterwards
      val releaseSubmission = Promise[Unit]()
      sequencer.setPolicy_("hold submission") {
        case submission if submission.batch.envelopes.exists(_.recipients.allRecipients.exists {
              case MemberRecipient(member) => member == defaultConfirmingParticipant(env).id
              case _ => false
            }) =>
          SendDecision.HoldBack(releaseSubmission.future)
        case _ => SendDecision.Process
      }
      loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.WARN))(
        {
          val (submissionId, ledgerEnd) =
            exec(prepared, Map(aliceE.partyId -> Seq(singleSignature)), epn)

          cpn.topology.party_to_key_mappings
            .sign_and_update(aliceE.partyId, env.daId, _.tryCopy(threshold = PositiveInt.two))

          releaseSubmission.success(())
          val completion = findCompletion(
            submissionId,
            ledgerEnd,
            aliceE,
            epn,
          )
          // Transaction should fail
          completion.status.value.code shouldBe Status.Code.INVALID_ARGUMENT.value()
        },
        LogEntry.assertLogSeq(
          localVerdictWarning ++ Seq(2, 3).map({ p =>
            (
              e => {
                e.warningMessage should (include(
                  s"Received 1 valid signatures from distinct keys (0 invalid), but expected at least 2 valid for ${aliceE.partyId}"
                ))
                e.mdc.get("participant") shouldBe Some(s"participant$p")
              },
              s"participant$p authentication",
            )
          }),
          Seq.empty,
        ),
      )
    }

    def testInvalidSignatures(
        assertion: (
            PrepareSubmissionResponse,
            Map[PartyId, Seq[Signature]],
            Promise[Unit],
        ) => Assertion,
        additionalExpectedLogs: Seq[(LogEntry => Assertion, String)] = Seq.empty,
    )(implicit env: FixtureParam): Unit = {
      import env.*

      val sequencer = getProgrammableSequencer(env.sequencer1.name)

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )
      val signatures = Map(
        aliceE.partyId -> global_secret.sign(
          prepared.preparedTransactionHash,
          aliceE,
          useAllKeys = true,
        )
      )

      // To bypass the checks in phase 1 we play a trick by holding back the submission request in the sequencer
      // while we change the signing keys, and release afterwards
      val releaseSubmission = Promise[Unit]()
      sequencer.setPolicy_("hold submission") {
        case submission if submission.batch.envelopes.exists(_.recipients.allRecipients.exists {
              case MemberRecipient(member) => member == defaultConfirmingParticipant(env).id
              case _ => false
            }) =>
          SendDecision.HoldBack(releaseSubmission.future)
        case _ => SendDecision.Process
      }
      loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.WARN))(
        assertion(prepared, signatures, releaseSubmission),
        LogEntry.assertLogSeq(
          additionalExpectedLogs ++
            Seq(2, 3).map({ p =>
              (
                e => {
                  e.warningMessage should (include(
                    s"Received 0 valid signatures from distinct keys (3 invalid), but expected at least 2 valid for ${aliceE.partyId}"
                  ))
                  e.mdc.get("participant") shouldBe Some(s"participant$p")
                },
                s"participant$p authentication",
              )
            }),
          Seq.empty,
        ),
      )
    }

    "fail if the signatures are invalid" in { implicit env =>
      testInvalidSignatures { (prepared, signatures, releaseSubmission) =>
        val (submissionId, ledgerEnd) = exec(prepared, signatures, epn)

        val newKeys = NonEmptyUtil.fromUnsafe(
          Set.fill(3)(
            env.global_secret.keys.secret.generate_key(usage = SigningKeyUsage.ProtocolOnly)
          )
        )

        // Change the protocol keys and threshold
        cpn.topology.party_to_key_mappings.sign_and_update(
          aliceE.partyId,
          env.daId,
          _.tryCopy(threshold = PositiveInt.two, signingKeys = newKeys),
        )

        // Update alice with the new keys for subsequent tests
        aliceE = aliceE.copy(signingFingerprints = newKeys.map(_.fingerprint).toSeq)
        releaseSubmission.success(())
        val completion = findCompletion(
          submissionId,
          ledgerEnd,
          aliceE,
          epn,
        )
        // Transaction should fail
        completion.status.value.code shouldBe Status.Code.INVALID_ARGUMENT.value()
      }
    }

    "fail execute and wait if the signatures are invalid" in { implicit env =>
      testInvalidSignatures(
        { (prepared, signatures, releaseSubmission) =>
          val response = Future(execAndWait(prepared, signatures))

          val newKeys = env.global_secret.keys.secret
            .generate_keys(PositiveInt.three, usage = SigningKeyUsage.ProtocolOnly)

          // Change the protocol keys
          cpn.topology.party_to_key_mappings.sign_and_update(
            aliceE.partyId,
            env.daId,
            _.tryCopy(threshold = PositiveInt.two, signingKeys = newKeys.toSet),
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
        Seq(
          (
            _.errorMessage should include(
              s"Request failed for participant2"
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
      val prepared = ppn.ledger_api.javaapi.interactive_submission
        .prepare(Seq(aliceE.partyId), Seq(pass.commands().loneElement))
      val signatures = Map(
        aliceE.partyId -> global_secret.sign(
          prepared.preparedTransactionHash,
          aliceE,
          useAllKeys = true,
        )
      )
      // This is only currently detected in phase III, at which point warnings are issued
      val completion =
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            val (submissionId, ledgerEnd) = exec(prepared, signatures, epn)
            findCompletion(submissionId, ledgerEnd, aliceE, epn)
          },
          LogEntry.assertLogSeq(
            Seq(2, 3).map({ p =>
              (
                e => {
                  e.warningMessage should (include regex "LOCAL_VERDICT_MALFORMED_REQUEST.*with a view that is not correctly authenticated")
                  e.mdc.get("participant") shouldBe Some(s"participant$p")
                },
                s"participant$p authentication",
              )
            })
          ),
        )
      completion.status.value.code shouldBe Status.Code.INVALID_ARGUMENT.value()
    }

    "fail with missing input contracts" in { implicit env =>
      import env.*
      import monocle.syntax.all.*

      // Set Alice back to threshold one
      cpn.topology.party_to_key_mappings.sign_and_update(
        aliceE.partyId,
        env.daId,
        _.tryCopy(threshold = PositiveInt.one),
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
          val participant = participant3.runningNode.value.getNode.value
          val routingSynchronizerState = participant.sync.getRoutingSynchronizerState
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
            .toScalaUnwrapped
            .futureValue
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              // This is logged during phase 3 when the CPN does not recognize the signature
              _.warningMessage should include("with a view that is not correctly authenticated"),
              "expected invalid signature check",
            )
          ),
          Seq.empty,
        ),
      )
    }
  }
}
