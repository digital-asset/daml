// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.toxiproxy.{ProxyConfig, SequencerToPostgres}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
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
      new UseBftSequencer(loggerFactory),
      QuickSequencerReconnection(loggerFactory),
      new UsePostgres(loggerFactory),
    )
}
