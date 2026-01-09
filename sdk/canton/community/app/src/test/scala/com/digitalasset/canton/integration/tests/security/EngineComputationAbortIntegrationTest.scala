// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.InactiveContracts
import com.digitalasset.canton.protocol.LocalRejectError.TimeRejects.LocalTimeout
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isMediatorResult
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.PartyId
import io.grpc.Status.Code
import monocle.macros.syntax.lens.*

import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/*
This test verifies the proper behavior of the feature that aborts an engine computation during phase 3.

Background: during phase 3, the participants perform a series of validations on the incoming request, which includes
the reinterpretation of the command (for transactions) and the recomputation of the stakeholders (for reassignments).
These engine computations don't need to run to completion if any of the following situations occurs:
- we concurrently receive a negative verdict from the mediator, indicating that other participants have already
  provided sufficient responses for the mediator to decide on the outcome;
- the request times out (past its decision time), whether we receive a verdict or not.
Therefore, the participant will abort the engine computation in these cases.

To recreate these scenarios without using very complex transaction that would take a long time to compute, we
simulate a very slow computation by inserting some "sleep" commands within the engine computation.
 */
sealed trait EngineComputationAbortIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with AcsInspection {
  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var alice: PartyId = _
  private var bob: PartyId = _
  private val engineHooks = new AtomicReference[Map[String, () => Unit]](Map.empty)

  private var programmableSequencer: ProgrammableSequencer = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.iterationsBetweenInterruptions).replace(10)
        )
      )
      .updateTestingConfig(
        _.focus(_.reinterpretationTestHookFor).replace(identifier =>
          engineHooks.get.getOrElse(identifier, () => ())
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        alice = participant1.parties.enable(
          "alice",
          synchronizeParticipants = Seq(participant2),
        )
        bob = participant2.parties.enable(
          "bob",
          synchronizeParticipants = Seq(participant1),
        )

        programmableSequencer = getProgrammableSequencer(sequencer1.name)
        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        participant1.uid
      }

  def slowDownEngineFor(participants: Seq[LocalParticipantReference]): Unit =
    engineHooks.set(
      participants
        .map(_.uid.identifier.str -> (() => { Threading.sleep(500) }))
        .toMap
    )

  private def createIOU(sender: LocalParticipantReference, payer: PartyId, owner: PartyId): Unit =
    sender.ledger_api.javaapi.commands.submit_async(
      actAs = Seq(alice),
      commands = IouSyntax.testIou(payer, owner).create().commands().asScala.toSeq,
    )

  private def setDecisionTime(
      duration: Duration
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    synchronizerOwners1.foreach { owner =>
      owner.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        update = params =>
          params.update(
            confirmationResponseTimeout = duration / 2,
            mediatorReactionTimeout = duration / 2,
          ),
      )
    }
  }

  "the engine reinterpretation" should {
    "abort when we receive a negative verdict" in { implicit env =>
      /*
      To test the first behavior, a simple scenario that would trigger a negative verdict is as follows:
        - P1 hosts Alice, P2 hosts Bob
        - Alice creates a contract C via P1 with Alice as signatory and Bob as observer
        - Alice archives C
        - Bob exercises a choice on C via P2, with P2 computing "slowly"

      As C is archived when Bob tries to use it, P1 rejects it during its validation, and that is sufficient for the mediator
      to return a negative "inactive contract" verdict.
      Unfortunately, it is not that simple: a well-behaved participant will notice during Phase 1 that the contract is
      inactive by checking its ACS and reject the submission before it even gets sequenced.

      We then use the following modified scenario:
        - Alice creates a contract C via P1 with Alice as signatory and Bob as observer
        - Alice dumps C as a binary blob and explicitly discloses it to Bob
        - Alice archives C
        - Bob exercises a choice on C via P2, providing C as a explicitly disclosed contract, with P2 computing "slowly"

      This time P1 does not reject the submission because C is provided as an explicitly disclosed contract, and as such
      does not have to be in its ACS.
       */
      import env.*

      slowDownEngineFor(Seq.empty)

      val iou = IouSyntax.createIou(participant1)(alice, bob)
      val iouDisclosed = {
        val iouCreated = participant1.ledger_api.state.acs
          .of_party(
            party = bob,
            filterTemplates = TemplateId.templateIdsFromJava(Iou.TEMPLATE_ID),
            includeCreatedEventBlob = true,
          )
          .find(_.event.contractId == iou.id.contractId)

        val synchronizerId = participant1.ledger_api.javaapi.event_query
          .by_contract_id(iou.id.contractId, Seq(bob))
          .getCreated
          .getSynchronizerId

        new DisclosedContract(
          iouCreated.value.event.createdEventBlob,
          synchronizerId,
          Optional.of(Iou.TEMPLATE_ID_WITH_PACKAGE_ID),
          Optional.of(iou.id.contractId),
        )
      }

      participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(alice),
        iou.id.exerciseArchive().commands().asScala.toSeq,
      )

      slowDownEngineFor(Seq(participant2))

      val ledgerEnd = participant2.ledger_api.state.end()

      val commandCompletion = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          participant2.ledger_api.javaapi.commands.submit_async(
            actAs = Seq(bob),
            iou.id.exerciseCall().commands().asScala.toSeq,
            disclosedContracts = Seq(iouDisclosed),
          )

          participant2.ledger_api.completions
            .list(bob, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEnd)
            .loneElement
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              { logEntry =>
                logEntry.warningMessage should (include("Aborting engine computation") and include(
                  s"received negative mediator verdict ParticipantReject"
                ) and include(InactiveContracts.id))
                logEntry.loggerName should include("/participant=participant2/")
              },
              "participant2's computation gets aborted",
            )
          )
        ),
      )

      val inactiveContracts = InactiveContracts.Reject(Seq.empty)
      val completionStatus = commandCompletion.status.value

      // The completion cause reflects the negative verdict
      completionStatus.code shouldBe Code.NOT_FOUND.value()
      completionStatus.message should include(InactiveContracts.id)
      completionStatus.message should include(inactiveContracts.cause)
    }

    "abort when we timeout" in { implicit env =>
      /*
      For this scenario, we shorten the synchronizer decision time and use the programmable sequencer to drop
      the mediator verdict. We then run the following scenario:
        - P1 hosts Alice, P2 hosts Bob
        - Alice creates a contract C via P1, with Alice as signatory and Bob as observer
        - P1 computes "slowly", P2 computes "normally"

      Since P1 never provides a response before the timeout, and the mediator never sends a verdict, P1's
      computation gets aborted.
       */
      import env.*

      setDecisionTime(3.seconds)
      slowDownEngineFor(Seq(participant1))

      programmableSequencer.setPolicy_("drop mediator results")(SendPolicy.processTimeProofs_ {
        submissionRequest =>
          if (isMediatorResult(submissionRequest))
            SendDecision.Drop
          else
            SendDecision.Process
      })

      val ledgerEnd = participant1.ledger_api.state.end()

      val commandCompletion = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          createIOU(sender = participant1, payer = alice, owner = bob)

          val completion = participant1.ledger_api.completions
            .list(alice, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEnd)
            .loneElement

          programmableSequencer.resetPolicy()
          // Run a ping to allow capturing all messages
          participant2.health.maybe_ping(participant2) shouldBe defined

          completion
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              { logEntry =>
                logEntry.warningMessage should (include("Aborting engine computation") and include(
                  s"has timed out"
                ))
                logEntry.loggerName should include("/participant=participant1/")
              },
              "participant1's computation gets aborted",
            )
          )
        ),
      )

      val localTimeout = LocalTimeout.Reject()
      val completionStatus = commandCompletion.status.value

      // The completion cause reflects the timeout
      completionStatus.code shouldBe Code.ABORTED.value()
      completionStatus.message should include(LocalTimeout.id)
      completionStatus.message should include(localTimeout.cause)
    }
  }
}

final class EngineComputationAbortIntegrationTestPostgres
    extends EngineComputationAbortIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
