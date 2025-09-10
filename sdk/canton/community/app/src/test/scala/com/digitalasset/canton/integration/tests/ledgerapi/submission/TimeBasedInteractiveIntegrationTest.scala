// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.admin.api.client.data.TemplateId.fromIdentifier
import com.digitalasset.canton.damltests.java.cycle.Cycle
import com.digitalasset.canton.damltests.java.statictimetest.Pass
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.defaultConfirmingParticipant
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.{ExternalParty, ForceFlags}
import com.digitalasset.canton.{HasExecutionContext, config}
import com.digitalasset.daml.lf.data.Time
import io.grpc.Status
import scalapb.TimestampConverters

import java.time.{Duration, Instant}
import scala.util.Random

/** Test interactive submission where the preparing, submitting and executing participants are all
  * independent.
  */
final class TimeBasedInteractiveIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils
    with HasExecutionContext {

  private val oneDay = Duration.ofHours(24)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }
      .addConfigTransforms(enableInteractiveSubmissionTransforms*)
      .addConfigTransform(ConfigTransforms.useStaticTime)

  private var aliceE: ExternalParty = _

  private def createPassCmd(
      ownerE: ExternalParty,
      id: String = "test-external-signing-id",
  ): Command = {
    val pass = new Pass(id, ownerE.toProtoPrimitive, Instant.now()) // This time is not used
    Command.fromJavaProto(pass.create.commands.loneElement.toProtoCommand)
  }

  private val ledgerTimeRecordTimeTolerance = Duration.ofSeconds(60)
  private val preparationTimeRecordTimeTolerance = Duration.ofHours(24)

  "Interactive submission" should {

    "onboard parties" in { implicit env =>
      import env.*

      env.sequencer1.topology.synchronizer_parameters.propose_update(
        env.daId,
        _.update(
          ledgerTimeRecordTimeTolerance =
            config.NonNegativeFiniteDuration(ledgerTimeRecordTimeTolerance),
          preparationTimeRecordTimeTolerance =
            config.NonNegativeFiniteDuration(preparationTimeRecordTimeTolerance),
          mediatorDeduplicationTimeout =
            config.NonNegativeFiniteDuration(preparationTimeRecordTimeTolerance.multipliedBy(2)),
        ),
        force = ForceFlags.all,
      )

      aliceE = participant3.parties.external.enable("Alice")
    }

    def createPassContract(implicit env: FixtureParam): (Pass.ContractId, DisclosedContract) = {
      val id = s"pass-${Random.nextLong()}"

      val pass = cpn.ledger_api.javaapi.commands.submit(
        Seq(aliceE),
        Seq(new Pass(id, aliceE.toProtoPrimitive, Instant.now()).create().commands().loneElement),
        includeCreatedEventBlob = true,
      )

      val event = pass.getEvents.asScalaProtoCreatedContracts.loneElement

      val disclosed = DisclosedContract(
        event.templateId,
        event.contractId,
        event.createdEventBlob,
        env.daId.logical.toProtoPrimitive,
      )

      (
        Pass.ContractId.fromContractId(
          new com.daml.ledger.javaapi.data.codegen.ContractId(event.contractId)
        ),
        disclosed,
      )
    }

    "respect explicit ledger time" in { implicit env =>
      import env.*

      val simClock = env.environment.simClock.value

      val (passCid, passContract) = createPassContract

      val command = Command.fromJavaProto(
        passCid.exerciseGetTime().commands().loneElement.toProtoCommand
      )
      val ledgerTimeSet = simClock.now.plus(oneDay).toInstant
      val prepared = ppn.ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(command),
        disclosedContracts = Seq(passContract),
        minLedgerTimeAbs = Some(ledgerTimeSet),
      )
      prepared.preparedTransaction.value.metadata.value.minLedgerEffectiveTime shouldBe Some(
        ledgerTimeSet.toEpochMilli * 1000
      )
      val signatures = Map(
        aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
      )

      simClock.advance(oneDay)
      execAndWait(prepared, signatures).discard
    }

    "ignore requested ledger time if getTime is not used" in { implicit env =>
      val simClock = env.environment.simClock.value
      val prepared = ppn.ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(createPassCmd(aliceE)),
        minLedgerTimeAbs = Some(simClock.now.toInstant.plusSeconds(20)),
      )
      prepared.preparedTransaction.value.metadata.value.minLedgerEffectiveTime shouldBe None
      prepared.preparedTransaction.value.metadata.value.maxLedgerEffectiveTime shouldBe None
    }

    "accept execution requests withing the submission tolerance" in { implicit env =>
      import env.*

      val simClock = env.environment.simClock.value
      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )
      val signatures = Map(
        aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
      )
      simClock.advance(preparationTimeRecordTimeTolerance.dividedBy(2))
      execAndWait(prepared, signatures).discard
    }

    "rejects execution requests outside the submission tolerance" in { implicit env =>
      import env.*
      val simClock = env.environment.simClock.value

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )
      val signatures = Map(
        aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
      )

      simClock.advance(preparationTimeRecordTimeTolerance.multipliedBy(2))

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val (submissionId, ledgerEnd) = exec(prepared, signatures, epn)
          findCompletion(
            submissionId,
            ledgerEnd,
            aliceE,
            epn,
          ).status.value.code shouldBe Status.Code.ABORTED.value()
        },
        entries => {
          forAtLeast(1, entries) { l =>
            l.warningMessage should (include regex "Time validation has failed: The delta of the preparation time .* and the record time .* exceeds the max of 24h")
          }
        },
      )

    }

    "use current time if ledger time is in the past" in { implicit env =>
      val simClock = env.environment.simClock.value

      val (passCid, passContract) = createPassContract

      val command = Command.fromJavaProto(
        passCid.exerciseGetTime().commands().loneElement.toProtoCommand
      )

      val ledgerTimeSet = simClock.now.toInstant.minusSeconds(20)
      val prepared =
        ppn.ledger_api.interactive_submission.prepare(
          Seq(aliceE.partyId),
          Seq(command),
          disclosedContracts = Seq(passContract),
          minLedgerTimeAbs = Some(ledgerTimeSet),
        )
      val ledgerTimeUsed = Time
        .Timestamp(prepared.preparedTransaction.value.metadata.value.minLedgerEffectiveTime.value)
        .toInstant
      ledgerTimeUsed should be > ledgerTimeSet

    }

    "set preparation time is set requested ledger effective time" in { implicit env =>
      val simClock = env.environment.simClock.value
      val command = createCycleCommand(aliceE, "test-external-signing")
      val expected = simClock.now.toInstant.plusSeconds(20)
      val prepared = ppn.ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(command),
        minLedgerTimeAbs = Some(expected),
      )
      val actual =
        Time.Timestamp(prepared.preparedTransaction.value.metadata.value.preparationTime).toInstant
      actual shouldBe expected
    }

    "modify requested ledger time if outside bounds" in { implicit env =>
      import env.*

      val simClock = env.environment.simClock.value

      val (passCid, passContract) = createPassContract

      val command = Command.fromJavaProto(
        passCid.exerciseGetTime().commands().loneElement.toProtoCommand
      )

      val expected = Time.Timestamp.assertFromInstant(simClock.now.toInstant)

      val prepared =
        ppn.ledger_api.interactive_submission.prepare(
          Seq(aliceE.partyId),
          Seq(command),
          disclosedContracts = Seq(passContract),
          minLedgerTimeAbs = Some(expected.toInstant),
        )

      prepared.getPreparedTransaction.getMetadata.getMinLedgerEffectiveTime shouldBe expected.micros
      prepared.getPreparedTransaction.getMetadata.getMaxLedgerEffectiveTime shouldBe expected.micros

      val signatures = Map(
        aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
      )

      simClock.advance(Duration.ofSeconds(10))

      val (submissionId, ledgerEnd) = exec(prepared, signatures, epn)

      val completion = findCompletion(submissionId, ledgerEnd, aliceE, epn)

      val updateFormat = getUpdateFormat(
        Set(aliceE.partyId),
        Seq(
          fromIdentifier(
            com.daml.ledger.api.v2.value.Identifier.fromJavaProto(Pass.TEMPLATE_ID.toProto)
          )
        ),
        TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      eventually() {
        val update: UpdateService.UpdateWrapper =
          defaultConfirmingParticipant(env).ledger_api.updates
            .update_by_id(completion.updateId, updateFormat)
            .value
        inside(update) {

          case w: TransactionWrapper =>
            TimestampConverters.asJavaInstant(
              w.transaction.effectiveAt.value
            ) shouldBe expected.toInstant
          case other =>
            fail(s"Did not expect: $other")
        }
      }

    }

  }
}
