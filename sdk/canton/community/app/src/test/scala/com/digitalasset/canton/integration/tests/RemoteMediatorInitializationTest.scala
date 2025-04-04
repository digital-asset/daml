// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.RemoteMediatorReference
import com.digitalasset.canton.crypto.{KeyPurpose, SigningPublicKeyWithName}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseExternalProcess,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}

class RemoteMediatorInitializationTest extends CommunityIntegrationTest with SharedEnvironment {
  val mediator1Name = "mediator1"
  val remoteMediator1Name = "mediator1-remote"

  protected def startAndGet(external: UseExternalProcess)(
      mediatorName: String
  )(implicit env: TestConsoleEnvironment): RemoteMediatorReference = {
    import env.*
    val mediator = rm(mediatorName)
    external.start(mediator.name)
    mediator
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0S1M1_Manual.withManualStart.withSetup(setup)

  protected val externalPlugin =
    new UseExternalProcess(
      loggerFactory,
      externalMediators = Set(mediator1Name),
      fileNameHint = this.getClass.getSimpleName,
    ) {
      // Force the remote name to be different from the local name
      override def localToRemoteNameTransformer(local: String): String = s"$local-remote"
    }

  registerPlugin(externalPlugin)
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  protected def setup(env: TestConsoleEnvironment): Unit = {
    import env.*

    val m1 = startAndGet(externalPlugin)(remoteMediator1Name)(env)

    m1.health.wait_for_ready_for_initialization()

    NetworkBootstrapper(
      Seq(
        NetworkTopologyDescription(
          daName,
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1),
          mediators = Seq(m1),
        )
      )
    )(env).bootstrap()
  }

  "A mediator bootstrapped from a remote console with a different name" must {
    "create only one signing key" in { implicit env =>
      import env.*

      val m1 = rm(remoteMediator1Name)

      val m1KeyNames = m1.keys.public
        .list(filterPurpose = Set(KeyPurpose.Signing))
        .collect { case SigningPublicKeyWithName(_, Some(name)) => name.unwrap }

      m1KeyNames should contain theSameElementsAs Seq(
        "mediator1-namespace",
        "mediator1-sequencer-auth",
        "mediator1-signing",
      )
    }
  }
}
