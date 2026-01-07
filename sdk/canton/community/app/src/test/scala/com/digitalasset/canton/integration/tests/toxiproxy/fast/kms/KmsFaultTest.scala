// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast.kms

import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ProxyConfig,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseConfigTransforms,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{CommunityIntegrationTest, SharedEnvironment}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level.INFO

import scala.concurrent.duration.*

trait KmsFaultTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase {

  protected def proxyConf: ProxyConfig
  protected def timeoutName: String
  protected def timeoutProxy: Long
  protected def downFor: Duration

  lazy val toxiProxyConf: UseToxiproxy.ToxiproxyConfig =
    UseToxiproxy.ToxiproxyConfig(List(proxyConf))
  lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)
  protected val getToxiProxy: () => RunningProxy = () =>
    toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value

  override def teardown(): Unit =
    ToxiproxyHelpers.removeAllProxies(
      toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
    )

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
  registerPlugin(
    new UseConfigTransforms(
      Seq(
        _.focus(_.parameters.nonStandardConfig)
          .replace(true)
      ),
      loggerFactory,
    )
  )
  registerPlugin(toxiproxyPlugin)

  s"With crypto provider" should {

    "retry through network failures and succeed" in { implicit env =>
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(INFO))(
        ToxiproxyHelpers.bongWithNetworkFailure(
          getToxiProxy(),
          proxy =>
            proxy.underlying
              .toxics()
              .timeout(timeoutName, ToxicDirection.UPSTREAM, timeoutProxy),
          levels = 0,
          downFor = downFor,
          recoveryCheck = ToxiproxyHelpers.pingAndLog(_),
          closeFunc = env => {
            import env.*
            participant1.stop()
            participant2.stop()

            sequencer1.stop()
            mediator1.stop()
          },
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              // Let's make sure we triggered the retry logic
              _.infoMessage should ((include("The operation 'signing with key") and include(
                "was not successful"
              )) or (include("The operation 'asymmetric decrypting") and include(
                "was not successful"
              ))),
              "expected AWS failure",
            )
          ),
          Seq(_ => assert(true)),
        ),
      )
    }
  }

}
