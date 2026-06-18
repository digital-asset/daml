// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.config.{DbConfig, KmsConfig, PositiveFiniteDuration}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase.mockKmsDriverConfig
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

/** TODO(#27529): In some scenarios clock advances still fails due to the current snapshot
  * approximation problems. For example, since participants rely on the current snapshot
  * approximation and can sign a message arbitrarily in the past, the verification by the sequencer
  * will fail if the nodes remain idle for a long time.
  *
  * Once everything is working, this test should be merged into
  * [[SessionSigningKeysIntegrationTest]], and session signing keys should be set as default again.
  */
trait SessionSigningKeysLifecycleIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase {

  protected val keyValidityDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(5)
  protected val advanceBy: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(3)

  override protected def otherConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.useStaticTime
  )

  "verify correct session key lifecycle with clock advances" in { implicit env =>
    import env.*

    val simClock = env.environment.simClock.value

    assertPingSucceeds(participant1, participant2)

    // session signing keys created are still valid
    simClock.advance(advanceBy.asJava)

    assertPingSucceeds(participant1, participant2)

    // session signing keys have expired; new keys will be generated
    simClock.advance(advanceBy.asJava)

    assertPingSucceeds(participant1, participant2)

  }

}

class MockKmsDriverSessionSigningKeysLifecycleIntegrationTestPostgres
    extends SessionSigningKeysLifecycleIntegrationTest
    with MockKmsDriverCryptoIntegrationTestBase {

  override protected val kmsConfig: KmsConfig =
    mockKmsDriverConfig.focus(_.sessionSigningKeys.keyValidityDuration).replace(keyValidityDuration)

  override protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] =
    Set.empty

  override protected lazy val protectedNodes: Set[String] =
    Set("sequencer1")

  setupPlugins(
    withAutoInit = true,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory),
  )
}
