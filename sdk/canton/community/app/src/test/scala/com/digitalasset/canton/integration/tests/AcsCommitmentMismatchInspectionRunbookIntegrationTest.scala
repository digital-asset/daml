// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.util.{CommitmentTestUtil, IntervalDuration}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch
import com.digitalasset.canton.participant.pruning.{CompareCmtContracts, ContractCreated}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.sun.jdi.request.InvalidRequestStateException
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference

trait AcsCommitmentMismatchInspectionRunbookIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with CommitmentTestUtil {

  private val interval: JDuration = JDuration.ofSeconds(5)
  private implicit val intervalDuration: IntervalDuration = IntervalDuration(interval)
  private val minObservationDuration = NonNegativeFiniteDuration.tryOfHours(1)
  private lazy val maxDedupDuration = java.time.Duration.ofHours(1)

  private val alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]] =
    new AtomicReference[Seq[Iou.Contract]](Seq.empty)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .updateTestingConfig(
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronisation.await_idle()
        sequencer2.topology.synchronisation.await_idle()
        initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
          initializedSynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                initializedSynchronizer.synchronizerId,
                _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
              )
          )
        }

        def connect(
            participant: ParticipantReference,
            minObservationDuration: NonNegativeFiniteDuration,
        ): Unit = {
          // Connect and disconnect so that we can modify the synchronizer connection config afterwards
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.synchronizers.disconnect_local(daName)
          val daConfig = participant.synchronizers.config(daName).value
          participant.synchronizers.connect_by_config(
            daConfig
              .focus(_.timeTracker.minObservationDuration)
              .replace(minObservationDuration.toConfig)
          )
        }

        connect(participant1, minObservationDuration)
        connect(participant2, minObservationDuration)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.foreach(_.dars.upload(CantonExamplesPath))
        passTopologyRegistrationTimeout(env)
      }

  "Commitment mismatch inspection runbook should work" in { implicit env =>
    import env.*

    deployThreeAndCheck(daId, alreadyDeployedContracts)

    logger.info("Open commitment on local participant")

    logger.debug(
      s"Create a commitment mismatch by purging one contract from P2's ACS."
    )
    participant2.synchronizers.disconnect_all()
    eventually() {
      participant2.synchronizers.list_connected() shouldBe empty
    }
    val purgedCidFromP2 = alreadyDeployedContracts.get().headOption.value.id.toLf
    participant2.repair.purge(daName, Seq(purgedCidFromP2))

    loggerFactory.assertLogsUnorderedOptional(
      {
        participant2.synchronizers.reconnect_all()
        eventually() {
          participant2.synchronizers
            .list_connected()
            .map(_.physicalSynchronizerId) should contain(daId)
        }

        val (_, period, commitment) = deployThreeAndCheck(daId, alreadyDeployedContracts)

        val synchronizerId1 = daId
        val mismatchTimestamp = period.toInclusive.forgetRefinement
        File.usingTemporaryFile(prefix = "open-commitment-local", suffix = ".json") { tmp =>
          val openCmtLocalFilename = tmp.pathAsString
          // user-manual-entry-begin: OpenCommitment
          val sentCommitmentP1 = participant1.commitments
            .lookup_sent_acs_commitments(
              synchronizerTimeRanges = Seq(
                SynchronizerTimeRange(
                  synchronizerId1,
                  Some(
                    TimeRange(
                      mismatchTimestamp.minusMillis(1),
                      mismatchTimestamp,
                    )
                  ),
                )
              ),
              counterParticipants = Seq(participant2.id),
              commitmentState = Seq.empty,
              verboseMode = true,
            )(synchronizerId1)
            .head
            .sentCommitment
            .getOrElse(throw new IllegalStateException("Commitment is empty"))
          val contractsAndReassignmentCountersP1 = participant1.commitments.open_commitment(
            commitment = sentCommitmentP1,
            physicalSynchronizerId = synchronizerId1,
            timestamp = mismatchTimestamp,
            counterParticipant = participant2,
            outputFile = Some(openCmtLocalFilename),
          )
          // user-manual-entry-end: OpenCommitment

          sentCommitmentP1 shouldBe commitment
          val returnedCids = contractsAndReassignmentCountersP1.map(c => c.cid)
          returnedCids should contain theSameElementsAs alreadyDeployedContracts
            .get()
            .map(c => c.id.toLf)

          logger.info("open commitment on counterParticipant")
          File.usingTemporaryFile(prefix = "open-commitment-remote", suffix = ".json") { tmp =>
            val openCmtRemoteFilename = tmp.pathAsString

            eventually() {
              participant2.commitments
                .computed(
                  daName,
                  mismatchTimestamp.toInstant.minusMillis(1),
                  mismatchTimestamp.toInstant,
                  Some(participant1),
                )
                .size shouldBe 1
            }
            // user-manual-entry-begin: OpenCommitmentCounter
            val sentCommitmentP2 = participant2.commitments
              .lookup_sent_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    synchronizerId1,
                    Some(
                      TimeRange(
                        mismatchTimestamp.minusMillis(1),
                        mismatchTimestamp,
                      )
                    ),
                  )
                ),
                counterParticipants = Seq(participant1.id),
                commitmentState = Seq.empty,
                verboseMode = true,
              )(synchronizerId1)
              .head
              .sentCommitment
              .getOrElse(throw new IllegalStateException("Commitment is empty"))
            val contractsAndReassignmentCountersP2 = participant2.commitments.open_commitment(
              commitment = sentCommitmentP2,
              physicalSynchronizerId = synchronizerId1,
              timestamp = mismatchTimestamp,
              counterParticipant = participant1,
              outputFile = Some(openCmtRemoteFilename),
            )
            // user-manual-entry-end: OpenCommitmentCounter

            import com.digitalasset.canton.integration.tests.util.CommitmentTestUtil.computeHashedCommitment
            val P2contracts = contractsAndReassignmentCountersP1.filter(_.cid != purgedCidFromP2)
            sentCommitmentP2 shouldBe computeHashedCommitment(P2contracts)
            val returnedCids2 = contractsAndReassignmentCountersP2.map(c => c.cid)
            returnedCids2 should contain theSameElementsAs alreadyDeployedContracts
              .get()
              .map(_.id.toLf)
              .filter(_ != purgedCidFromP2)

            logger.info("validate locally the opened commitment by the counterParticipant")
            eventually() {
              participant1.commitments
                .received(
                  daName,
                  mismatchTimestamp.toInstant.minusMillis(1),
                  mismatchTimestamp.toInstant,
                  Some(participant2),
                )
                .size shouldBe 1
            }
            // user-manual-entry-begin: ValidateContractMetadata
            import com.digitalasset.canton.integration.tests.util.CommitmentTestUtil.computeHashedCommitment
            import com.digitalasset.canton.participant.pruning.OpenCommitmentHelper
            val remoteContractsAndTransferCounters =
              OpenCommitmentHelper.readFromFile(openCmtRemoteFilename)
            val receivedCommitmentP1fromP2 = participant1.commitments
              .lookup_received_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    synchronizerId1,
                    Some(
                      TimeRange(
                        mismatchTimestamp.minusMillis(1),
                        mismatchTimestamp,
                      )
                    ),
                  )
                ),
                counterParticipants = Seq(participant2.id),
                commitmentState = Seq.empty,
                verboseMode = true,
              )(synchronizerId1)
              .head
              .receivedCommitment
              .getOrElse(throw new IllegalStateException("Commitment is empty"))
            val receivedOpenedCommitmentP1fromP2 =
              computeHashedCommitment(remoteContractsAndTransferCounters)
            if (receivedOpenedCommitmentP1fromP2 != receivedCommitmentP1fromP2)
              throw new InvalidRequestStateException(
                s"The commitment does not match the opened contract metadata!"
              )
            // user-manual-entry-end: ValidateContractMetadata

            logger.info("identify mismatching contracts")

            File.usingTemporaryFile(prefix = "mismatching-contracts", suffix = ".json") { tmp =>
              val mismatchingContractsFilename = tmp.pathAsString
              // user-manual-entry-begin: MismatchingContracts
              import com.digitalasset.canton.participant.pruning.CommitmentContractMetadata
              import com.digitalasset.canton.participant.pruning.OpenCommitmentHelper
              val contractsAndTransferCounters1 =
                OpenCommitmentHelper.readFromFile(openCmtLocalFilename)
              val contractsAndTransferCountersCounterParticipant1 =
                OpenCommitmentHelper.readFromFile(openCmtRemoteFilename)
              val mismatchingContracts = CommitmentContractMetadata.compare(
                contractsAndTransferCounters1,
                contractsAndTransferCountersCounterParticipant1,
              )
              mismatchingContracts.writeToFile(mismatchingContractsFilename)
              // user-manual-entry-end: MismatchingContracts

              mismatchingContracts.cidsOnlyLocal should contain theSameElementsAs Seq(
                purgedCidFromP2
              )
              mismatchingContracts.cidsOnlyRemote shouldBe empty
              mismatchingContracts.differentReassignmentCounters shouldBe empty

              logger.info("inspect mismatch cause")

              File.usingTemporaryFile(prefix = "inspect-contracts", suffix = ".json") { tmp =>
                val inspectContractsFilename = tmp.pathAsString
                // user-manual-entry-begin: InspectMismatchCause
                import com.digitalasset.canton.participant.pruning.CommitmentInspectContract
                val mismatches = CompareCmtContracts.readFromFile(mismatchingContractsFilename)

                val inspectContracts = participant1.commitments.inspect_commitment_contracts(
                  contracts = mismatches.cidsOnlyLocal ++ mismatches.differentReassignmentCounters,
                  timestamp = mismatchTimestamp,
                  expectedSynchronizerId = synchronizerId1,
                  downloadPayload = true,
                )
                CommitmentInspectContract.writeToFile(inspectContractsFilename, inspectContracts)
                // user-manual-entry-end: InspectMismatchCause

                inspectContracts.filter(s =>
                  s.activeOnExpectedSynchronizer &&
                    s.contract.isDefined &&
                    s.state.sizeIs == 1 &&
                    s.state.count(cs =>
                      cs.contractState
                        .isInstanceOf[ContractCreated] && cs.synchronizerId == daId.logical
                    ) == 1
                ) should have size Seq(purgedCidFromP2).size.toLong
              }
            }
          }
        }

        logger.info("test cleanup")

        logger.debug(
          s"Rectify the commitment mismatch by purging the contract from P1's ACS."
        )
        participant1.synchronizers.disconnect_all()
        eventually() {
          participant1.synchronizers.list_connected() shouldBe empty
        }
        participant1.repair.purge(daName, Seq(purgedCidFromP2))
        participant1.synchronizers.reconnect_all()
        eventually() {
          participant1.synchronizers
            .list_connected()
            .map(_.physicalSynchronizerId) should contain(daId)
        }
        deployThreeAndCheck(daId, alreadyDeployedContracts)
      },
      (
        LogEntryOptionality.OptionalMany -> (_.warningMessage should include(
          CommitmentsMismatch.id
        )),
      ),
    )
  }
}

class AcsCommitmentMismatchInspectionRunbookIntegrationTestPostgres
    extends AcsCommitmentMismatchInspectionRunbookIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}

//class AcsCommitmentMismatchInspectionRunbookIntegrationTestH2
//    extends AcsCommitmentMismatchInspectionRunbookIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer(
//        Seq(
//          Set(InstanceName.tryCreate("sequencer1")),
//          Set(InstanceName.tryCreate("sequencer2")),
//        )
//      ),
//    )
//  )
//}
