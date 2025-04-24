// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  MediatorReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SynchronizerWithoutMediatorError
import com.digitalasset.canton.sequencing.protocol.{
  ClosedEnvelope,
  MediatorGroupRecipient,
  Recipients,
  SequencerErrors,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}
import com.digitalasset.canton.topology.{ForceFlag, MediatorId}
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}

trait MultipleMediatorsBaseTest { this: BaseTest with HasProgrammableSequencer =>

  protected def participantSeesMediators(
      ref: ParticipantReference,
      expectedActive: Set[Set[MediatorId]], // For each group, the mediators
  ): Assertion = eventually() {
    ref.topology.mediators
      .list()
      .sortBy(_.item.group)
      .map(_.item.active.toSet.forgetNE)
      .toSet shouldBe expectedActive
  }

  protected def uploadAndWaitForMediatorIdentity(
      mediator: MediatorReference,
      sequencer: SequencerReference,
  ): Assertion = {
    val med2Identity = mediator.topology.transactions.identity_transactions()

    sequencer.topology.transactions.load(
      med2Identity,
      sequencer.synchronizer_id,
      ForceFlag.AlienMember,
    )
    eventually() {
      sequencer.topology.transactions
        .list(
          store = TopologyStoreId.Synchronizer(sequencer.synchronizer_id),
          filterNamespace = mediator.namespace.filterString,
        )
        .result
        .map(_.transaction.mapping.code)
        .toSet shouldBe Set(OwnerToKeyMapping.code, NamespaceDelegation.code)
    }
  }

  protected def switchMediatorDuringSubmission[T](
      sequencer: LocalSequencerReference,
      oldMediatorGroup: NonNegativeInt,
      newMediatorGroup: NonNegativeInt,
      newMediator: MediatorReference,
      participant: LocalParticipantReference,
  )(
      submit: () => T
  )(implicit ec: ExecutionContext): Future[T] = {
    val progSequencer = getProgrammableSequencer(sequencer.name)

    val participantId = participant.id // Do not inline as this is a gRPC call

    val promiseNewMediatorActive = Promise[Unit]()
    val promiseSubmissionOnHold = Promise[Unit]()

    val synchronizerId = sequencer.synchronizer_id

    uploadAndWaitForMediatorIdentity(newMediator, sequencer)

    progSequencer.setPolicy(s"delay all submissions from $participantId")(
      SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
        if (submissionRequest.sender == participantId && submissionRequest.isConfirmationRequest) {
          logger.debug("Received request from participant1")
          promiseSubmissionOnHold.trySuccess(())
          SendDecision.HoldBack(promiseNewMediatorActive.future)
        } else SendDecision.Process
      }
    )

    val submissionF = Future {
      val error = SequencerErrors
        .SubmissionRequestRefused(
          s"The following mediator groups do not exist ${List(oldMediatorGroup)}"
        )
        .cause

      val errorUnknownSender = SequencerErrors
        .SenderUnknown(
          s"(Eligible) Senders are unknown: MED::"
        )
        .cause

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        submit(),
        logEntries => {
          logEntries.size should be <= 4
          if (logEntries.sizeIs == 4) {
            logEntries.head.errorMessage should include(
              errorUnknownSender
            ) // GrpcRequestRefusedByServer: FAILED_PRECONDITION/SEQUENCER_SENDER_UNKNOWN
            logEntries(1).warningMessage should include(error) // warning
            // failure of the submission when no tracking is used
            logEntries(2).warningMessage should include(error)
          } else if (logEntries.sizeIs == 3) {
            // we can have either GrpcRequestRefusedByServer: FAILED_PRECONDITION/SEQUENCER_SENDER_UNKNOWN and warning,
            // or warning and failure of the submission when no tracking is used
            if (logEntries.head.level == Level.ERROR)
              logEntries.head.errorMessage should include(errorUnknownSender)
            else logEntries.head.warningMessage should include(error)
            logEntries(1).warningMessage should include(error)
          } else if (logEntries.sizeIs == 2) {
            logEntries.head.warningMessage should include(error)
          } else succeed
          logEntries.last.errorMessage should include(error) // failure of console command
        },
      )
    }

    val switchOutMediatorF = promiseSubmissionOnHold.future.map { _ =>
      logger.debug("Add a new mediator group")
      sequencer.mediators.groups.propose_new_group(
        newMediatorGroup,
        PositiveInt.one,
        active = Seq(newMediator),
      )

      logger.debug("Remove the old mediator group")
      sequencer.topology.mediators.remove_group(synchronizerId, oldMediatorGroup)

      logger.debug("Switched out the mediator")
      promiseNewMediatorActive.success(())
    }

    switchOutMediatorF.futureValue

    progSequencer.resetPolicy()

    submissionF
  }
}

class MultipleMediatorsIntegrationTest
    extends CommunityIntegrationTest
    with HasCycleUtils
    with IsolatedEnvironments
    with HasProgrammableSequencer
    with MultipleMediatorsBaseTest
    with OperabilityTestHelpers {

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Manual

  private def startNodes()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participants.local.start()
    sequencers.local.start()
    mediators.local.start()
  }

  private def bootstrapSynchronizer(
      mediators: Seq[LocalMediatorReference]
  )(implicit env: TestConsoleEnvironment): Unit =
    new NetworkBootstrapper(
      NetworkTopologyDescription(
        env.daName,
        synchronizerOwners = Seq(env.sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(env.sequencer1),
        mediators = mediators,
      )
    )(env).bootstrap()

  private val remedy = operabilityTest("Mediator")("Sequencer") _

  "A synchronizer with multiple mediators" when_ { setting =>
    "the synchronizer is not initialised" must_ { cause =>
      remedy(setting)(cause)(
        "Support initialising with multiple external mediators"
      ) in { implicit env =>
        import env.*

        startNodes()

        forAll(Seq(mediator1, mediator2))(_.health.initialized() shouldBe false)

        mediator1.id should not be mediator2.id

        bootstrapSynchronizer(Seq(mediator1, mediator2))

        participant1.synchronizers.connect_local(sequencer1, daName)

        participantSeesMediators(participant1, Set(Set(mediator1.id, mediator2.id)))

        participant1.health.ping(participant1)
      }

      remedy(setting)(cause)(
        "Support initialising second mediator subsequently"
      ) in { implicit env =>
        import env.*

        startNodes()
        bootstrapSynchronizer(Seq(mediator1))
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.health.ping(participant1)

        uploadAndWaitForMediatorIdentity(mediator2, sequencer1)

        sequencer1.mediators.groups.propose_new_group(
          MediatorGroupIndex.one,
          PositiveInt.one,
          active = Seq(mediator2),
        )

        participantSeesMediators(participant1, Set(Set(mediator1.id), Set(mediator2.id)))

        participant1.health.ping(participant1)

        // We check that the mediators are used evenly by looking at the results they are sending via the sequencer
        val mediator1MessageCount = new AtomicInteger(0)
        val mediator2MessageCount = new AtomicInteger(0)
        val sequencer = getProgrammableSequencer(sequencer1.name)
        sequencer.setPolicy_("count requests sent to mediator")(
          SendPolicy.processTimeProofs_ { submissionRequest =>
            if (
              submissionRequest.sender == participant1.id && submissionRequest.isConfirmationRequest
            ) {
              def findMediator(recipients: Recipients): Option[MediatorGroupRecipient] =
                recipients.asSingleGroup.value.collectFirst { case med: MediatorGroupRecipient =>
                  med
                }

              val mediator = submissionRequest.batch.envelopes.collectFirst {
                case ClosedEnvelope(_bytes, recipients, _) if findMediator(recipients).nonEmpty =>
                  findMediator(recipients).value
              }
              mediator match {
                case Some(MediatorGroupRecipient(MediatorGroupIndex.zero)) =>
                  mediator1MessageCount.incrementAndGet().discard
                case Some(MediatorGroupRecipient(MediatorGroupIndex.one)) =>
                  mediator2MessageCount.incrementAndGet().discard
                case _ =>
              }
            }
            SendDecision.Process
          }
        )
        val pingCount = 10
        Future
          .traverse((1 to pingCount)) { i =>
            Future {
              participant1.health.ping(participant1, id = s"mediator-round-robin-ping-$i")
            }
          }
          .futureValue
        sequencer.resetPolicy()

        // Since pings are retried internally, we don't know the exact number of expected responses sent by the mediators
        val mediator1Count = mediator1MessageCount.get()
        val mediator2Count = mediator2MessageCount.get()

        val total = mediator1Count + mediator2Count
        total shouldBe >=(pingCount * 2) // 2 requests per ping
        mediator1Count shouldBe <=(total / 2 + 1)
        mediator2Count shouldBe <=(total / 2 + 1)

        // also connect another participant such that we can be sure that the second mediator observes changes
        participant2.synchronizers.connect_local(sequencer1, daName)
        participant2.health.ping(participant2)
      }

    }

    "all mediators are disabled" must_ { cause =>
      remedy(setting)(cause)(
        "participant fails gracefully upon submission"
      ) in { implicit env =>
        import env.*

        startNodes()
        bootstrapSynchronizer(Seq(mediator1))
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.health.ping(participant1)

        sequencer1.topology.mediators.remove_group(daId, NonNegativeInt.zero)

        eventually() {
          participant1.topology.mediators
            .list(daId, group = Some(NonNegativeInt.one)) shouldBe empty
        }

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          createCycleContract(participant1, participant1.adminParty, "no-mediator-on-synchronizer"),
          _.errorMessage should include(SynchronizerWithoutMediatorError.code.id),
        )

        uploadAndWaitForMediatorIdentity(mediator2, sequencer1)

        sequencer1.mediators.groups.propose_new_group(
          NonNegativeInt.one,
          PositiveInt.one,
          active = Seq(mediator2),
        )
        eventually() {
          participant1.topology.mediators
            .list(daId, group = Some(NonNegativeInt.one)) should not be empty
        }

        createCycleContract(participant1, participant1.adminParty, "mediator-reactivated")
      }
    }

    "the mediator is switched out during transaction submission" must_ { cause =>
      remedy(setting)(cause)("resubmit") in { implicit env =>
        import env.*

        startNodes()
        bootstrapSynchronizer(Seq(mediator1))
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.health.ping(participant1)

        val submissionF = switchMediatorDuringSubmission(
          sequencer = sequencer1,
          oldMediatorGroup = NonNegativeInt.zero,
          newMediatorGroup = NonNegativeInt.one,
          newMediator = mediator2,
          participant = participant1,
        ) { () =>
          a[CommandFailure] should be thrownBy {
            createCycleContract(
              participant1,
              participant1.adminParty,
              id = "offboarded-mediator",
              commandId = "offboarded-mediator-command",
            )
          }
        }
        logger.debug(
          "Make sure that the participant receives another timestamp that triggers the rejection"
        )

        // A plain fetch_synchronizer_time isn't enough because the SynchronizerTimeTracker may use the delayed submission
        // as the witness for the time
        createCycleContract(participant1, participant1.adminParty, "use-new-mediator")
        submissionF.futureValue
      }
    }
  }
}
