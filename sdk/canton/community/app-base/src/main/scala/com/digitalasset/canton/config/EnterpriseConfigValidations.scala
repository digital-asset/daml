// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.Validated
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.CommunityConfigValidations.formatNodeList
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig

object EnterpriseConfigValidations extends ConfigValidations {
  private val Valid: Validated[NonEmpty[Seq[String]], Unit] = Validated.valid(())

  override protected val validations: List[Validation] =
    List[CantonConfig => Validated[NonEmpty[Seq[String]], Unit]](
      dbLockFeaturesRequireUsingLockSupportingStorage,
      highlyAvailableSequencerTotalNodeCount,
      noDuplicateStorageUnlessReplicated,
      atLeastOneNode,
      sequencerClientRetryDelays,
    ) ++ CommunityConfigValidations.genericValidations

  def isUsingHaSequencer(config: SequencerConfig): Boolean =
    PartialFunction.cond(config) {
      // needs to be using both a database sequencer config with that set to use HA
      case dbConfig: SequencerConfig.Database =>
        dbConfig.highAvailabilityEnabled
    }

  private def sequencerClientRetryDelays(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val CantonConfig(
      sequencers,
      mediators,
      participants,
      _,
      _,
      _,
      _,
      _,
      _,
    ) = config

    val sequencerClientConfigs =
      (Map.empty[
        InstanceName,
        LocalNodeConfig,
      ] ++ sequencers ++ mediators ++ participants).view
        .mapValues(_.sequencerClient)
        .toMap

    def checkSequencerClientConfig(
        ref: InstanceName,
        config: SequencerClientConfig,
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Validated.cond(
        config.initialConnectionRetryDelay.underlying <= config.warnDisconnectDelay.underlying &&
          config.warnDisconnectDelay.underlying <= config.maxConnectionRetryDelay.underlying,
        (),
        NonEmpty(
          Seq,
          s"Retry delay configuration for '$ref' sequencer client doesn't respect the condition initialConnectionRetryDelay <= warnDisconnectDelay <= maxConnectionRetryDelay; " +
            s"respective values are (${config.initialConnectionRetryDelay.underlying}, ${config.warnDisconnectDelay.underlying}, ${config.maxConnectionRetryDelay.underlying})",
        ),
      )

    sequencerClientConfigs.toSeq.traverse_ { case (ref, sequencerClientConfig) =>
      checkSequencerClientConfig(ref, sequencerClientConfig)
    }
  }

  private def highlyAvailableSequencerTotalNodeCount(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def validate(
        nodeType: String,
        name: String,
        sequencerConfig: SequencerConfig,
    ): Validated[NonEmpty[Seq[String]], Unit] =
      Option(sequencerConfig)
        .collect { case dbConfig: SequencerConfig.Database => dbConfig }
        .filter(_.highAvailabilityEnabled)
        .map(
          _.highAvailability
            .getOrElse(throw new IllegalStateException("HA not set despite being enabled"))
            .totalNodeCount
            .unwrap
        )
        .fold(Valid) { totalNodeCount =>
          Validated.cond(
            totalNodeCount < DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE,
            (),
            NonEmpty(
              Seq,
              s"$nodeType node $name sets sequencer HA total node count to $totalNodeCount. Must be less than ${DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE}.",
            ),
          )
        }

    config.sequencersByString
      .map { case (name, config) =>
        validate("Sequencer", name, config.sequencer)
      }
      .toList
      .sequence_
  }

  /** If a participant is using replicated storage or the database sequencer is configured to use
    * its high availability support then it requires using a database that supports locking.
    */
  private def dbLockFeaturesRequireUsingLockSupportingStorage(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val sequencerHaFeature = "the highly available database sequencer"

    def verifySupportedStorage(
        nodeType: String,
        nodeName: String,
        feature: String,
        config: LocalNodeConfig,
    ): Option[String] =
      Option.when(!DbLockConfig.isSupportedConfig(config.storage))(
        s"$nodeType node $nodeName must be configured to use Postgres for storage to use $feature"
      )

    val sequencerNodeValidationErrors = config.sequencersByString
      .filter { case (_, sequencerNodeConfig) =>
        isUsingHaSequencer(sequencerNodeConfig.sequencer)
      }
      .toSeq
      .mapFilter { case (name, node) =>
        verifySupportedStorage("Sequencer", name, sequencerHaFeature, node)
      }

    val participantNodeValidations = config.participantsByString
      .filter(
        _._2.replication.exists(_.isEnabled)
      ) // only replicated participants need to use lock based storage
      .toSeq
      .mapFilter { case (name, node) =>
        verifySupportedStorage("Participant", name, "replication", node)
      }

    toValidated(sequencerNodeValidationErrors ++ participantNodeValidations)
  }

  /** Validate the config that the storage configuration is not shared between nodes unless for
    * replicated participants.
    */
  private def noDuplicateStorageUnlessReplicated(
      config: CantonConfig
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    def allReplicatedParticipants(nodes: List[(String, LocalNodeConfig)]): Boolean =
      nodes
        .map(_._2)
        .forall(PartialFunction.cond(_) { case cfg: LocalParticipantConfig =>
          cfg.replication.exists(_.isEnabled)
        })

    def allHighlyAvailableSequencers(nodes: List[(String, LocalNodeConfig)]): Boolean =
      nodes
        .map(_._2)
        .forall(PartialFunction.cond(_) { case cfg: SequencerNodeConfig =>
          isUsingHaSequencer(cfg.sequencer)
        })

    def allHighlyAvailableMediators(nodes: List[(String, LocalNodeConfig)]): Boolean =
      nodes
        .map(_._2)
        .forall(PartialFunction.cond(_) { case cfg: MediatorNodeConfig =>
          cfg.replicationEnabled
        })

    def requiresSharedStorage(nodes: List[(String, LocalNodeConfig)]): Boolean =
      allReplicatedParticipants(nodes) || allHighlyAvailableSequencers(
        nodes
      ) || allHighlyAvailableMediators(nodes)

    val dbAccessToNodes = CommunityConfigValidations.extractNormalizedDbAccess(
      config.participantsByString,
      config.sequencersByString,
      config.mediatorsByString,
    )

    val errors = dbAccessToNodes.toSeq
      .mapFilter {
        case (dbAccess, nodes) if nodes.sizeIs > 1 && !requiresSharedStorage(nodes) =>
          Option(s"Nodes ${formatNodeList(nodes)} share same DB access: $dbAccess")
        case _ => None
      }

    toValidated(errors)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def atLeastOneNode(c: CantonConfig): Validated[NonEmpty[Seq[String]], Unit] = {
    val CantonConfig(
      sequencers,
      mediators,
      participants,
      remoteSequencers,
      remoteMediators,
      remoteParticipants,
      _,
      _,
      _,
    ) = c
    Validated.cond(
      Seq(
        participants,
        remoteParticipants,
        sequencers,
        remoteSequencers,
        mediators,
        remoteMediators,
      ).exists(_.nonEmpty),
      (),
      NonEmpty(Seq, "At least one node must be defined in the configuration"),
    )
  }

}
