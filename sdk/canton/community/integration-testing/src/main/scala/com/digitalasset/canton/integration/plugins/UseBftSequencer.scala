// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{CantonConfig, TlsClientConfig}
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.{
  MultiSynchronizer,
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.util.SingleUseCell
import monocle.macros.syntax.lens.*

import scala.collection.mutable

final class UseBftSequencer(
    override protected val loggerFactory: NamedLoggerFactory,
    val sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
    dynamicallyOnboardedSequencerNames: Seq[InstanceName] = Seq.empty,
) extends EnvironmentSetupPlugin {

  val sequencerEndpoints
      : SingleUseCell[Map[InstanceName, BftBlockOrdererConfig.P2PEndpointConfig]] =
    new SingleUseCell()

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    // Contains all sequencers from the environment definition. Typically, the environment definition also contains
    //  sequencers that are onboarded dynamically by tests (i.e, not initialized from the very beginning).
    val groups = sequencerGroups match {
      case MultiSynchronizer(groups) => groups
      case SingleSynchronizer => Seq(config.sequencers.keys)
    }
    val sequencersToEndpoints: mutable.Map[InstanceName, BftBlockOrdererConfig.P2PEndpointConfig] =
      mutable.Map()
    val sequencersToConfig: Map[InstanceName, SequencerConfig] =
      groups.flatMap { group =>
        val endpoints = group.map { sequencerName =>
          sequencerName -> BftBlockOrdererConfig.P2PEndpointConfig(
            "localhost",
            UniquePortGenerator.next,
            Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
          )
        }.toMap
        endpoints.map { case (selfInstanceName, selfEndpoint) =>
          sequencersToEndpoints.addOne(selfInstanceName -> selfEndpoint)
          val otherInitialEndpoints =
            if (dynamicallyOnboardedSequencerNames.contains(selfInstanceName))
              // Dynamically onboarded peers' endpoints are not part of the initial network but are added later
              // by the concrete test case.
              Seq.empty
            else
              endpoints.view
                .filterNot { case (name, _) =>
                  name == selfInstanceName || dynamicallyOnboardedSequencerNames.contains(name)
                }
                .values
                .toSeq
          val network = BftBlockOrdererConfig.P2PNetworkConfig(
            BftBlockOrdererConfig.P2PServerConfig(
              selfEndpoint.address,
              internalPort = Some(selfEndpoint.port),
              externalAddress = selfEndpoint.address,
              externalPort = selfEndpoint.port,
              externalTlsConfig = Some(
                TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)
              ),
            ),
            peerEndpoints = otherInitialEndpoints,
          )
          selfInstanceName -> SequencerConfig.BftSequencer(
            config = BftBlockOrdererConfig(initialNetwork = Some(network))
          )
        }
      }.toMap

    def mapSequencerConfigs(
        kv: (InstanceName, SequencerNodeConfig)
    ): (InstanceName, SequencerNodeConfig) = kv match {
      case (name, cfg) =>
        (
          name,
          cfg.focus(_.sequencer).replace(sequencersToConfig(name)),
        )
    }

    sequencerEndpoints.putIfAbsent(sequencersToEndpoints.toMap)
    config
      .focus(_.sequencers)
      .modify(_.map(mapSequencerConfigs))
  }
}
