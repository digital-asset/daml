// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.CloseableTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.{LogEntry, NamedLogging, SuppressingLogger}
import com.digitalasset.canton.metrics.{MetricsFactoryType, ScopedInMemoryMetricsFactory}
import org.scalatest.{Assertion, BeforeAndAfterAll, Suite}

import scala.util.control.NonFatal

/** Provides an ability to create a canton environment when needed for test.
  * Include [[IsolatedEnvironments]] or [[SharedEnvironment]] to determine when this happens.
  * Uses [[ConcurrentEnvironmentLimiter]] to ensure we limit the number of concurrent environments in a test run.
  */
sealed trait EnvironmentSetup[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends BeforeAndAfterAll {
  this: Suite with HasEnvironmentDefinition[E, TCE] with NamedLogging =>

  private lazy val envDef = environmentDefinition

  // plugins are registered during construction from a single thread
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var plugins: Seq[EnvironmentSetupPlugin[E, TCE]] = Seq()

  protected[integration] def registerPlugin(plugin: EnvironmentSetupPlugin[E, TCE]): Unit =
    plugins = plugins :+ plugin

  override protected def beforeAll(): Unit = {
    plugins.foreach(_.beforeTests())
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally plugins.foreach(_.afterTests())
  }

  /** Provide an environment for an individual test either by reusing an existing one or creating a new one
    * depending on the approach being used.
    */
  def provideEnvironment: TCE

  /** Optional hook for implementors to know when a test has finished and be provided the environment instance.
    * This is required over a afterEach hook as we need the environment instance passed.
    */
  def testFinished(environment: TCE): Unit = {}

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  def logsToBeHandledAtStartup: Option[Seq[LogEntry] => Assertion] = None

  protected def handleStartupLogs[T](start: => T): T =
    logsToBeHandledAtStartup
      .map(assertion => loggerFactory.assertLoggedWarningsAndErrorsSeq(start, assertion))
      .getOrElse(start)

  /** Creates a new environment manually for a test without concurrent environment limitation and with optional config transformation.
    *
    * @param initialConfig specifies which configuration to start from with the default being a NEW one created
    *                      from the current environment.
    * @param configTransform a function that applies changes to the initial configuration
    *                        (with the plugins applied on top)
    * @param runPlugins a function that expects a plugin reference and returns whether or not it's supposed to be run
    *                   against the initial configuration
    * @return a new test console environment
    */
  protected def manualCreateEnvironment(
      initialConfig: E#Config = envDef.generateConfig,
      configTransform: E#Config => E#Config = identity,
      runPlugins: EnvironmentSetupPlugin[E, TCE] => Boolean = _ => true,
  ): TCE = {
    val testConfig = initialConfig
    // note: beforeEnvironmentCreate may well have side-effects (e.g. starting databases or docker containers)
    val pluginConfig = {
      plugins.foldLeft(testConfig)((config, plugin) =>
        if (runPlugins(plugin))
          plugin.beforeEnvironmentCreated(config)
        else
          config
      )
    }
    val finalConfig = configTransform(pluginConfig)

    val scopedMetricsFactory = new ScopedInMemoryMetricsFactory
    val environmentFixture =
      envDef.environmentFactory.create(
        finalConfig,
        loggerFactory,
        envDef.testingConfig.copy(
          metricsFactoryType =
            /* If metrics reporters were configured for the test then it's an externally observed test
             * therefore actual metrics have to be reported.
             * The in memory metrics are used when no reporters are configured and the metrics are
             * observed directly in the test scenarios.
             * */
            if (finalConfig.monitoring.metrics.reporters.isEmpty)
              MetricsFactoryType.InMemory(scopedMetricsFactory.forContext)
            else MetricsFactoryType.External,
          initializeGlobalOpenTelemetry = false,
        ),
      )

    try {
      val testEnvironment: TCE =
        envDef.createTestConsole(environmentFixture, loggerFactory)

      plugins.foreach(plugin =>
        if (runPlugins(plugin)) plugin.afterEnvironmentCreated(finalConfig, testEnvironment)
      )

      if (!finalConfig.parameters.manualStart)
        handleStartupLogs(testEnvironment.startAll())

      envDef.networkBootstrapFactory(testEnvironment).bootstrap()

      envDef.setups.foreach(setup => setup(testEnvironment))

      testEnvironment
    } catch {
      case NonFatal(ex) =>
        // attempt to ensure the environment is shutdown if anything in the startup of initialization fails
        try {
          environmentFixture.close()
        } catch {
          case NonFatal(shutdownException) =>
            // we suppress the exception thrown by the shutdown as we want to propagate the original
            // exception, however add it to the suppressed list on this thrown exception
            ex.addSuppressed(shutdownException)
        }
        // rethrow exception
        throw ex
    }
  }

  protected def createEnvironment(): TCE =
    ConcurrentEnvironmentLimiter.create(getClass.getName, numPermits)(
      manualCreateEnvironment()
    )

  protected def manualDestroyEnvironment(environment: TCE): Unit = {
    val config = environment.actualConfig
    plugins.foreach(_.beforeEnvironmentDestroyed(config, environment))
    try {
      environment.close()
    } finally {
      envDef.teardown(())
      plugins.foreach(_.afterEnvironmentDestroyed(config))
    }
  }

  protected def destroyEnvironment(environment: TCE): Unit = {
    ConcurrentEnvironmentLimiter.destroy(getClass.getName, numPermits) {
      manualDestroyEnvironment(environment)
    }
  }

  /** number of permits required by this test
    *
    * this can be used for heavy tests to ensure that we have less other tests running concurrently
    */
  protected def numPermits: PositiveInt = PositiveInt.one

}

/** Starts an environment in a beforeAll test and uses it for all tests.
  * Destroys it in an afterAll hook.
  */
trait SharedEnvironment[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends EnvironmentSetup[E, TCE]
    with CloseableTest {
  this: Suite with HasEnvironmentDefinition[E, TCE] with NamedLogging =>

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var sharedEnvironment: Option[TCE] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sharedEnvironment = Some(createEnvironment())
  }

  override def afterAll(): Unit =
    try {
      sharedEnvironment.foreach(destroyEnvironment)
    } finally super.afterAll()

  override def provideEnvironment: TCE =
    sharedEnvironment.getOrElse(
      sys.error("beforeAll should have run before providing a shared environment")
    )
}

/** Creates an environment for each test. */
trait IsolatedEnvironments[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends EnvironmentSetup[E, TCE] {
  this: Suite with HasEnvironmentDefinition[E, TCE] with NamedLogging =>

  override def provideEnvironment: TCE = createEnvironment()
  override def testFinished(environment: TCE): Unit = destroyEnvironment(environment)
}
