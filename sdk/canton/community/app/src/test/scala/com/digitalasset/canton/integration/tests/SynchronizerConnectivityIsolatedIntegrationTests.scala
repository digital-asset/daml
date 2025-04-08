// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.google.protobuf.ByteString

trait SynchronizerConnectivityIsolatedIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasCycleUtils {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1_S1M1

  "connecting a single participant terminates only when the connected synchronizer is active" in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.active(daName) shouldBe true

      participant2.synchronizers.connect_by_config(
        SynchronizerConnectionConfig(
          daName,
          SequencerConnections.single(sequencer1.sequencerConnection),
        )
      )
      participant2.synchronizers.active(daName) shouldBe true
      participant2.synchronizers.disconnect(daName)
      participant2.synchronizers.active(daName) shouldBe false

      participant2.synchronizers.reconnect_all()
      participant2.synchronizers.active(daName) shouldBe true

      participant3.synchronizers.connect_by_config(
        SynchronizerConnectionConfig(
          daName,
          SequencerConnections.single(sequencer1.sequencerConnection),
          manualConnect = true,
        )
      )
      participant3.synchronizers.active(daName) shouldBe true
  }

  "connecting multiple participants terminates only when the connected synchronizer is active" in {
    implicit env =>
      import env.*

      Seq(participant1, participant2).synchronizers.connect_local(sequencer1, alias = daName)
      Seq(participant1, participant2).foreach(_.synchronizers.active(daName) shouldBe true)

      Seq(participant3, participant4).synchronizers
        .register(
          SynchronizerConnectionConfig(
            daName,
            SequencerConnections.single(sequencer1.sequencerConnection),
          )
        )
      Seq(participant3, participant4).foreach(_.synchronizers.active(daName) shouldBe false)
      Seq(participant3, participant4).synchronizers.reconnect_all()
      Seq(participant3, participant4).foreach(_.synchronizers.active(daName) shouldBe true)

      val acmeCertificate = sequencer2.sequencerConnection match {
        case grpc: GrpcSequencerConnection =>
          grpc.customTrustCertificates.getOrElse(ByteString.EMPTY)
        case _ => fail()
      }
      Seq(participant1, participant2).synchronizers
        .register(
          SynchronizerConnectionConfig(
            acmeName,
            SequencerConnections.single(sequencer2.sequencerConnection),
            manualConnect = true,
          )
            .withCertificates(SequencerAlias.Default, acmeCertificate)
        )
      Seq(participant1, participant2).foreach(_.synchronizers.active(acmeName) shouldBe false)
      Seq(participant1, participant2).synchronizers.reconnect(acmeName)
      Seq(participant1, participant2).foreach(_.synchronizers.active(acmeName) shouldBe true)
  }
}

//class SynchronizerConnectivityIsolatedReferenceIntegrationTestDefault
//  extends SynchronizerConnectivityIsolatedIntegrationTests

class SynchronizerConnectivityIsolatedReferenceIntegrationTestPostgres
    extends SynchronizerConnectivityIsolatedIntegrationTest {
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

class SynchronizerConnectivityIsolatedBftOrderingIntegrationTestPostgres
    extends SynchronizerConnectivityIsolatedIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
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
