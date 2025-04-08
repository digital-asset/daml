// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.ConfigTransforms.heavyTestDefaults
import com.digitalasset.canton.integration.IntegrationTestUtilities.{GrabbedCounts, grabCounts}
import com.digitalasset.canton.integration.plugins.toxiproxy.RunningProxy
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.Toxic
import org.scalatest.Assertion

import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.Try

/** Utility object for Toxiproxy tests.
  */
object ToxiproxyHelpers extends BaseTest with EntitySyntax {
  override val defaultParticipant: String = "participant1"

  val environmentDefinitionDefault: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .clearConfigTransforms() // Clear the config transforms as the default config transforms reduce timeouts and therefore cause flakes
      .addConfigTransforms(heavyTestDefaults*)

  val bongTimeout: FiniteDuration = 2.minute
  val defaultBongLevel = 4

  def bongWithToxic(
      getProxy: => RunningProxy,
      mkToxic: List[RunningProxy => Toxic],
      levels: Int = defaultBongLevel,
      cleanClose: Boolean = true,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    logger.info(s"Performing a bong with a toxic")

    val proxy: RunningProxy = getProxy
    val toxics = mkToxic.map(f => f(proxy))

    try {
      val bong = participant1.testing.bong(
        Set(participant2.id),
        levels = levels,
        timeout = bongTimeout,
      )
      logger.info(s"bong completes in $bong")
    } finally {
      toxics.foreach { t =>
        logger.info(s"Removing ${t.getName}")
        t.remove()
      }
      waitNoToxics(proxy)

      if (cleanClose)
        closeMembers(proxy)(env)
    }
  }

  def waitNoToxics(proxy: RunningProxy): Unit =
    eventually() {
      assertNoToxics(proxy)
    }

  def assertNoToxics(proxy: RunningProxy): Unit = {
    import scala.jdk.CollectionConverters.*
    val client = proxy.controllingToxiproxyClient
    val proxies = client.getProxies.asScala.toList
    val toxics = proxies.flatMap(p => p.toxics().getAll.asScala.toList.map(t => t.getName))
    if (toxics.nonEmpty) {
      logger.info(s"Observed toxics $toxics from proxies ${proxies.map(p => p.getName)}")
    }
    toxics shouldBe List.empty
  }

  private def closeFuncSimple(env: TestConsoleEnvironment): Unit = {
    import env.*
    participant1.stop()
    participant2.stop()

    mediator1.stop()
    sequencer1.stop()
  }

  def bongWithNetworkFailure(
      getProxy: => RunningProxy,
      failure: RunningProxy => Toxic,
      downFor: Duration,
      timeout: FiniteDuration = bongTimeout,
      levels: Int = defaultBongLevel,
      recoveryCheck: TestConsoleEnvironment => Unit = _ => (),
      catchBongErrors: PartialFunction[Throwable, Assertion] = Map.empty,
      cleanClose: Boolean = true,
      closeFunc: TestConsoleEnvironment => Unit = closeFuncSimple,
  )(implicit env: TestConsoleEnvironment): Unit = {

    import env.*
    val proxy = getProxy

    val bong: Future[Duration] =
      bongAndWaitForProgress(timeout, levels, daName)

    Threading.sleep(500)
    logger.info(s"Connection becoming toxic")
    val toxic = failure(proxy)
    logger.info(s"Connection is toxic")

    failFor(
      proxy,
      downFor,
      timeout,
      bong,
      { () =>
        toxic.remove()
        waitNoToxics(proxy)
      },
      recoveryCheck,
      catchBongErrors,
      cleanClose,
      closeFunc,
    )
  }

  def bongWithKillConnection(
      getProxy: => RunningProxy,
      downFor: Duration,
      levels: Int = defaultBongLevel,
      timeout: FiniteDuration = bongTimeout,
      recoveryCheck: TestConsoleEnvironment => Unit = _ => (),
      catchBongErrors: PartialFunction[Throwable, Assertion] = Map.empty,
      cleanClose: Boolean = true,
  )(implicit env: TestConsoleEnvironment): Unit = {

    import env.*

    val proxy = getProxy

    val bong = bongAndWaitForProgress(timeout, levels, daName)

    logger.info(s"Taking down the connection by toxiproxy")
    proxy.underlying.disable()
    logger.info(s"Connection is down")

    failFor(
      proxy,
      downFor,
      timeout,
      bong,
      () => {
        proxy.underlying.enable()
      },
      recoveryCheck,
      catchBongErrors,
      cleanClose,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  private def failFor(
      proxy: RunningProxy,
      downFor: Duration,
      timeout: Duration,
      bong: Future[Duration],
      recover: () => Unit,
      recoveryCheck: TestConsoleEnvironment => Unit,
      catchErrors: PartialFunction[Throwable, Assertion],
      cleanClose: Boolean,
      closeFunc: TestConsoleEnvironment => Unit = closeFuncSimple,
  )(implicit env: TestConsoleEnvironment): Assertion =
    if (downFor.isFinite) {
      try {
        // Fail for the specified duration
        Threading.sleep(downFor.toMillis, 0)
        recoverAndLog(recover)
        logger.info(s"Testing recovery with a ping")
        pingAndLog(env)

        // Await the completion of the bong
        val result = Try(awaitBongAndLog(timeout, bong, catchErrors))

        // Check the system isn't in a weird state
        recoveryCheck(env)

        // Return the result of the bong
        result.get
      } finally {
        if (cleanClose) {
          closeMembers(proxy, closeFunc)
        }
      }

    } else {
      try {
        // Await the completion of the bong
        val result = Try(awaitBongAndLog(timeout, bong, catchErrors))

        // Recover the system after the bong has completed
        recoverAndLog(recover)

        // Check the system isn't in a weird state
        recoveryCheck(env)

        // Return the result of the bong
        result.get
      } finally {
        if (cleanClose)
          closeMembers(proxy, closeFunc)
      }
    }

  private def bongAndWaitForProgress(
      timeout: FiniteDuration,
      levels: Int,
      synchronizerAlias: SynchronizerAlias,
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val countsBefore: GrabbedCounts = grabCounts(synchronizerAlias, participant1, limit = 1000)
    val expectedCount = IntegrationTestUtilities.expectedGrabbedCountsForBong(levels)
    val stopAfter = Math.floor(expectedCount.pcsCount * 0.1 + countsBefore.pcsCount).toInt

    logger.info(s"Sending a bong")
    val bong = Future {
      participant1.testing.bong(
        Set(participant1.id, participant2.id),
        levels = levels,
        timeout = timeout,
      )

    }

    // Let the bong make some progress before taking down the connection
    eventually(
      FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS),
      maxPollInterval = 200.millis,
    ) {
      val current: GrabbedCounts = grabCounts(synchronizerAlias, participant1, limit = 1000)
      current.pcsCount should be >= stopAfter
      logger.info(s"Reached pcs count ${current.pcsCount} >= $stopAfter. Bong may be interrupted.")
    }
    bong
  }

  private def awaitBongAndLog(
      timeout: Duration,
      bong: Future[Duration],
      catchErrors: PartialFunction[Throwable, Assertion],
  ): Assertion =
    try {
      // Use Await.ready instead of Await.result so you can work out where a TimeoutException comes from
      val completed: Future[Duration] =
        try {
          Await.ready(bong, timeout + 1.minute)
        } catch {
          case _: TimeoutException => fail(s"Command not completed within timeout.")
        }

      @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
      val bongD: Duration = completed.value.value.get
      logger.info(s"Toxiproxy: completed a bong in $bongD")
      succeed
    } catch {
      catchErrors
    }

  private def recoverAndLog(recover: () => Unit): Unit = {
    logger.info(s"Beginning recovery")
    recover()
    logger.info(s"Recovery complete")
  }

  def closeMembers(
      proxy: RunningProxy,
      closeFunc: TestConsoleEnvironment => Unit = closeFuncSimple,
  )(implicit env: TestConsoleEnvironment): Unit = {

    import scala.jdk.CollectionConverters.*
    logger.info(s"Toxiproxy: shutting down all members")
    try {
      closeFunc(env)
    } finally {
      proxy.underlying.delete()
      val client = proxy.controllingToxiproxyClient
      eventually() {
        val proxies = client.getProxies.asScala.toList.map(p => p.getName)
        proxies shouldBe List.empty
      }
    }
  }

  def pingAndLog(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    // Check the system has recovered
    eventually(bongTimeout * 2) {
      logger.info(s"Running a ping to see if the system has recovered")
      val pingTime = participant1.health.maybe_ping(participant2, timeout = 10.seconds)
      pingTime shouldBe defined
      logger.info(s"Recovery ping: p1 pings p2 in $pingTime")

      // Perform a second ping, as the duration may drop significantly which can be helpful when diagnosing flakes
      val pingTime2 = participant1.health.maybe_ping(participant2, timeout = 10.seconds)
      pingTime2 shouldBe defined
      logger.info(s"Second recovery ping: p1 pings p2 in $pingTime2")
    }
  }

  def checkLogs(expectedProblems: List[String])(logs: Seq[LogEntry]): Assertion =
    forEvery(logs) { x =>
      assert(
        expectedProblems.exists(msg => x.toString.contains(msg)),
        s"line $x contained unexpected problem",
      )
    }

  def removeAllProxies(client: ToxiproxyClient): Unit = {
    import scala.jdk.CollectionConverters.*
    logger.info(s"Removing all proxies")

    val proxies = client.getProxies.asScala.toList
    proxies.foreach { p =>
      p.delete()
    }

    eventually() {
      val proxies = client.getProxies.asScala.toList.map(p => p.getName)
      logger.info(s"Proxies are $proxies")
      proxies shouldBe List.empty
    }
  }
}
