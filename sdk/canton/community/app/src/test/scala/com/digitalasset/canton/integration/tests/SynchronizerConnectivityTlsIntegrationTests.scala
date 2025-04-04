// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import monocle.macros.syntax.lens.*

trait SynchronizerConnectivityTlsIntegrationTests
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasCycleUtils {

  private val certChainFile = PemFile(
    ExistingFile.tryCreate("enterprise/app/src/test/resources/tls/public-api.crt")
  )
  private val privateKeyFile = PemFile(
    ExistingFile.tryCreate("enterprise/app/src/test/resources/tls/public-api.pem")
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.tls).replace(
            Some(
              TlsBaseServerConfig(
                certChainFile = certChainFile,
                privateKeyFile = privateKeyFile,
              )
            )
          )
        )
      )

  "Connect to a synchronizer with TLS" in { implicit env =>
    import env.*

    val hostname = "localhost"
    val port = sequencer1.config.publicApi.port.unwrap
    val certs = certChainFile.pemFile.unwrap.getPath

    // architecture-handbook-entry-begin: TlsConnect
    participant1.synchronizers.connect(
      synchronizerAlias = daName,
      connection = s"https://$hostname:$port",
      certificatesPath = certs, // path to certificate chain file (.pem) of server
    )
    // architecture-handbook-entry-end: TlsConnect

    // self ping should work (and warm-up the system)
    eventually() {
      assertPingSucceeds(participant1, participant1)
    }
  }

}

//class SynchronizerConnectivityTlsReferenceIntegrationTestsDefault
//  extends SynchronizerConnectivityTlsIntegrationTests

class SynchronizerConnectivityTlsReferenceIntegrationTestsPostgres
    extends SynchronizerConnectivityTlsIntegrationTests {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
}
