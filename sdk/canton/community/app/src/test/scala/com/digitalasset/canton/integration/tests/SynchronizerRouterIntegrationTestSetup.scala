// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.daml.ledger.javaapi.data.{Command, Transaction, TransactionTree}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.damltests.java.automaticreassignmenttransactions.{
  Aggregate,
  Inject,
  Single,
}
import com.digitalasset.canton.damltests.java.test.Dummy
import com.digitalasset.canton.integration.util.{AcsInspection, EntitySyntax}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper

import java.util.{List as JList, Optional}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

trait SynchronizerRouterIntegrationTestSetup
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with AcsInspection {

  import SynchronizerRouterIntegrationTestSetup.*

  private val reconciliationInterval = PositiveSeconds.tryOfDays(100000)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .P4_S1M1_S1M1_S1M1()
      .withSetup { implicit env =>
        import env.*

        initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
          initializedSynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              // Very high value to essentially disable the ACS commitment processor
              // The ACS commitment processor tends to get confused by disabled participants.
              .propose_update(
                initializedSynchronizer.synchronizerId,
                _.update(reconciliationInterval = reconciliationInterval.toConfig),
              )
          )
        }
      }

  protected val darPath: String = CantonTestsPath

  protected val synchronizer1: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer1")
  protected val synchronizer2: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer2")
  protected val synchronizer3: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer3")
  protected val allSynchronizers: List[SynchronizerAlias] =
    List(synchronizer1, synchronizer2, synchronizer3)

  protected var party1Id: PartyId = _
  protected var party1aId: PartyId = _
  protected var party1bId: PartyId = _
  protected var party1cId: PartyId = _
  protected var party2Id: PartyId = _
  protected var party3Id: PartyId = _
  protected var party4Id: PartyId = _

  protected def connectToDefaultSynchronizers()(implicit
      env: TestConsoleEnvironment
  ): Unit =
    connectToCustomSynchronizers(
      Map(
        env.participant1 -> allSynchronizers.toSet,
        env.participant2 -> Set(synchronizer2, synchronizer3),
        env.participant3 -> Set(synchronizer3),
      )
    )

  protected def connectToCustomSynchronizers(
      synchronizersByParticipant: Map[LocalParticipantReference, Set[SynchronizerAlias]]
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    for ((participant, synchronizers) <- synchronizersByParticipant) {
      val connectedSynchronizers = participant.synchronizers.list_connected()
      val synchronizersToConnectTo =
        synchronizers -- connectedSynchronizers.map(_.synchronizerAlias.unwrap)
      val synchronizersToDisconnectFrom =
        connectedSynchronizers.map(_.synchronizerAlias).toSet -- synchronizers

      synchronizersToDisconnectFrom.foreach { synchronizerAlias =>
        logger.debug(s"Disconnecting $participant from synchronizer $synchronizerAlias")
        val compatibleSequencer = s(s"sequencer${synchronizerAlias.unwrap.last}")
        // Some test needs a specific topology where the participant is disabled from the synchronizer.
        // Since we cannot disable a participant, we change the synchronizer permission to Observation.
        // It's enough because the contracts involved in the tests require at least confirmation permission.
        compatibleSequencer.topology.participant_synchronizer_permissions.propose(
          initializedSynchronizers(synchronizerAlias).synchronizerId,
          participant.id,
          permission = ParticipantPermission.Observation,
        )
        participant.synchronizers.disconnect_local(synchronizerAlias)
      }
      synchronizersToConnectTo.foreach { synchronizerAlias =>
        logger.debug(s"Connecting $participant to $synchronizerAlias")
        val compatibleSequencer = s(s"sequencer${synchronizerAlias.unwrap.last}")
        participant.synchronizers.connect_local(compatibleSequencer, alias = synchronizerAlias)
        // We set back the synchronizer permission to Submission
        compatibleSequencer.topology.participant_synchronizer_permissions.propose(
          initializedSynchronizers(synchronizerAlias).synchronizerId,
          participant.id,
          permission = ParticipantPermission.Submission,
        )
      }
    }

    synchronizeTopologyState()
  }

  protected def synchronizeTopologyState()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    logger.info("Start synchronizing topology state...")
    participants.all.foreach { participant =>
      participant.testing.fetch_synchronizer_times()
    }
    logger.info(s"Topology state has been synchronized.")
  }

  protected def createInject(
      owner: PartyId,
      synchronizerId: Option[SynchronizerId],
      participant: LocalParticipantReference,
  ): Inject.Contract =
    runCommand(
      Inject.COMPANION,
      owner,
      synchronizerId,
      participant,
      new Inject(owner.toProtoPrimitive, owner.toProtoPrimitive).create.commands,
    )

  protected def exerciseInForm(
      participant: ParticipantReference,
      owner: PartyId,
      submitter: PartyId,
      contract: Inject.Contract,
      targetSynchronizer: Option[SynchronizerId] = None,
  ): Inject.Contract = {
    val cmd = contract.id.exerciseInform(submitter.toProtoPrimitive).commands.asScala.toSeq
    val tree =
      participant.ledger_api.javaapi.commands.submit_flat(Seq(owner), cmd, targetSynchronizer)

    JavaDecodeUtil
      .decodeAllCreated(Inject.COMPANION)(tree)
      .loneElement
  }

  protected def createDummy(
      owner: PartyId,
      synchronizerId: Option[SynchronizerId],
      participant: LocalParticipantReference,
  ): Dummy.Contract = {
    val createCmd = new Dummy(owner.toProtoPrimitive).create.commands
    runCommand(Dummy.COMPANION, owner, synchronizerId, participant, createCmd)
  }

  protected def exerciseDummies(
      submitters: Iterable[PartyId],
      dummies: Iterable[Dummy.Contract],
      participant: ParticipantReference,
      targetSynchronizer: Option[SynchronizerId] = None,
  ): TransactionTree = {
    val exerciseCmds =
      dummies.map(dummy => dummy.id.exerciseDummyChoice().commands.asScala.toSeq).toSeq.flatten
    val firstSubmitter = submitters.headOption.value
    val createCmd =
      new Single(
        firstSubmitter.toProtoPrimitive,
        firstSubmitter.toProtoPrimitive,
      ).create.commands.asScala.toSeq

    participant.ledger_api.javaapi.commands.submit(
      submitters.toSeq,
      exerciseCmds ++ createCmd,
      targetSynchronizer,
    )
  }
}

private[tests] object SynchronizerRouterIntegrationTestSetup {
  def runCommand[
      ID <: ContractId[?],
      TC <: Contract[ID, ?],
  ](
      companion: ContractCompanion[TC, ID, ?],
      owner: PartyId,
      synchronizerId: Option[SynchronizerId],
      participant: ParticipantReference,
      commands: JList[Command],
  ): TC = {
    val tree =
      participant.ledger_api.javaapi.commands.submit_flat(
        Seq(owner),
        commands.asScala.toSeq,
        synchronizerId = synchronizerId,
      )

    val contract = JavaDecodeUtil
      .decodeAllCreated(companion)(tree)
      .loneElement

    participant match {
      case participant: LocalParticipantReference =>
        synchronizerId.foreach { synchronizerId =>
          AcsInspection.assertInLedgerAcsSync(
            List(participant),
            owner,
            synchronizerId,
            contract.id.toLf,
          )
        }
      case _ => ()
    }
    contract
  }

  def createAggregate(
      participant: ParticipantReference,
      submitter: PartyId,
      singles: Seq[Single.ContractId],
      synchronizerId: Option[SynchronizerId],
      additionalParty: Option[PartyId] = None,
  ): Aggregate.Contract = {
    val createCmd = new Aggregate(
      submitter.toProtoPrimitive,
      singles.asJava,
      Optional.ofNullable(additionalParty.map(_.toProtoPrimitive).orNull),
    ).create.commands
    runCommand(
      Aggregate.COMPANION,
      submitter,
      synchronizerId,
      participant,
      createCmd,
    )
  }

  def exerciseCountAll(
      participant: ParticipantReference,
      submitter: PartyId,
      contract: Aggregate.Contract,
      targetSynchronizer: Option[SynchronizerId] = None,
      optTimeout: Option[NonNegativeDuration] = Some(
        NonNegativeDuration.tryFromDuration(60.seconds)
      ),
  ): TransactionTree = {
    val exerciseCmd =
      contract.id.exerciseCountAll().commands.asScala.toSeq
    val tree =
      participant.ledger_api.javaapi.commands.submit(
        Seq(submitter),
        exerciseCmd,
        targetSynchronizer,
        optTimeout = optTimeout,
      )
    tree
  }

  def exerciseCountPublicAll(
      participant: ParticipantReference,
      submitter: PartyId,
      contract: Aggregate.Contract,
      targetSynchronizer: Option[SynchronizerId] = None,
      optTimeout: Option[NonNegativeDuration] = Some(
        NonNegativeDuration.tryFromDuration(60.seconds)
      ),
  ): Transaction = {
    val exerciseCmd =
      contract.id.exerciseCountPublicAll().commands.asScala.toSeq
    val tree =
      participant.ledger_api.javaapi.commands.submit_flat(
        Seq(submitter),
        exerciseCmd,
        targetSynchronizer,
        optTimeout = optTimeout,
      )
    tree
  }

  def createSingle(
      owner: PartyId,
      synchronizerId: Option[SynchronizerId],
      participant: LocalParticipantReference,
      coordinator: PartyId,
  ): Single.Contract = {
    val createCmd =
      new Single(
        owner.toProtoPrimitive,
        coordinator.toProtoPrimitive,
      ).create.commands
    runCommand(Single.COMPANION, owner, synchronizerId, participant, createCmd)
  }
}
