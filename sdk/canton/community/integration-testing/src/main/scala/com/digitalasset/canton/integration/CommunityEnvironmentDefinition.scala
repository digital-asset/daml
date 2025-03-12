// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import better.files.{File, Resource}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.{
  CantonConfig,
  CommunityCantonEdition,
  CryptoConfig,
  TestingConfigInternal,
}
import com.digitalasset.canton.console.{ConsoleEnvironment, TestConsoleOutput}
import com.digitalasset.canton.environment.{
  CommunityEnvironmentFactory,
  Environment,
  EnvironmentFactory,
}
import com.digitalasset.canton.integration.CommunityTests.CommunityTestConsoleEnvironment
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.typesafe.config.ConfigFactory
import monocle.macros.syntax.lens.*

final case class CommunityEnvironmentDefinition(
    override val baseConfig: CantonConfig,
    override val testingConfig: TestingConfigInternal,
    override val setups: List[CommunityTestConsoleEnvironment => Unit] = Nil,
    override val teardown: Unit => Unit = _ => (),
    override val configTransforms: Seq[ConfigTransform],
) extends BaseEnvironmentDefinition[CommunityTestConsoleEnvironment](
      baseConfig,
      testingConfig,
      setups,
      teardown,
      configTransforms,
    ) {

  def withManualStart: CommunityEnvironmentDefinition =
    copy(baseConfig = baseConfig.focus(_.parameters.manualStart).replace(true))

  def withSetup(setup: CommunityTestConsoleEnvironment => Unit): CommunityEnvironmentDefinition =
    copy(setups = setups :+ setup)

  def clearConfigTransforms(): CommunityEnvironmentDefinition = copy(configTransforms = Seq())

  def addConfigTransforms(
      transforms: ConfigTransform*
  ): CommunityEnvironmentDefinition =
    transforms.foldLeft(this)((ed, ct) => ed.addConfigTransform(ct))

  def addConfigTransform(
      transform: ConfigTransform
  ): CommunityEnvironmentDefinition =
    copy(configTransforms = this.configTransforms :+ transform)

  override lazy val environmentFactory: EnvironmentFactory =
    CommunityEnvironmentFactory

  override def createTestConsole(
      environment: Environment,
      loggerFactory: NamedLoggerFactory,
  ): TestConsoleEnvironment =
    new ConsoleEnvironment(
      environment,
      new TestConsoleOutput(loggerFactory),
    ) with TestEnvironment {
      override val actualConfig: CantonConfig = this.environment.config
    }
}

object CommunityEnvironmentDefinition {
  lazy val defaultStaticSynchronizerParameters: StaticSynchronizerParameters =
    StaticSynchronizerParameters.defaults(
      CryptoConfig(),
      BaseTest.testedProtocolVersion,
    )

  /** Read configuration from files
    *
    * Use this method if your configuration files contain nested includes (which silently fail to
    * include with fromResource)
    */
  def fromFiles(files: File*): CommunityEnvironmentDefinition = {
    val config =
      CantonConfig.parseAndLoadOrExit(files.map(_.toJava), CommunityCantonEdition)
    CommunityEnvironmentDefinition(
      baseConfig = config,
      configTransforms = Seq(),
      testingConfig = TestingConfigInternal(),
    )
  }
  lazy val simpleTopology: CommunityEnvironmentDefinition =
    fromResource("examples/01-simple-topology/simple-topology.conf")

  def fromResource(path: String): CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition(
      baseConfig = loadConfigFromResource(path),
      testingConfig = TestingConfigInternal(),
      configTransforms = Seq(),
    )

  private def loadConfigFromResource(path: String): CantonConfig = {
    val rawConfig = ConfigFactory.parseString(Resource.getAsString(path))
    CantonConfig.loadOrExit(rawConfig, CommunityCantonEdition)
  }
}
