// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.toxiproxy.{ProxyConfig, SequencerToPostgres}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.sequencer.{
  QuickSequencerReconnection,
  ToxiproxyIntegrationTest,
}

class ReferenceToxiproxyIntegrationTest extends ToxiproxyIntegrationTest {
  override protected def component: String = "postgres"

  override def proxyConfs: Seq[ProxyConfig] = List("sequencer1", "sequencer2")
    .map(sequencerName => SequencerToPostgres(s"$sequencerName-to-postgres", sequencerName))

  override def pluginsToRegister: Seq[EnvironmentSetupPlugin] =
    Seq(
      // TODO(#29603): change this back to use the BFT Orderer.
      // The test was flaking a lot with the BFT Orderer
      // registerPlugin(new UseBftSequencer(loggerFactory))
      new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
      QuickSequencerReconnection(loggerFactory),
      new UsePostgres(loggerFactory),
    )
}
