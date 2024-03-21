// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.lifecycle.ShutdownFailedException
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.CommunityDbMigrationsFactory
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.NodeId
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

class NodesTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  val clock = new SimClock(loggerFactory = loggerFactory)
  trait TestNode extends CantonNode
  case class TestNodeConfig()
      extends LocalNodeConfig
      with ConfigDefaults[DefaultPorts, TestNodeConfig] {
    override val init: InitConfig = InitConfig()
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig()
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory()
    override val crypto: CommunityCryptoConfig = CommunityCryptoConfig()
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig()
    override val caching: CachingConfigs = CachingConfigs()
    override val nodeTypeName: String = "test-node"
    override def clientAdminApi = adminApi.clientConfig
    override def withDefaults(ports: DefaultPorts): TestNodeConfig = this
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig()
    override val topologyX: TopologyXConfig = TopologyXConfig.NotUsed
    override def parameters: LocalNodeParametersConfig = new LocalNodeParametersConfig {
      override def batching: BatchingConfig = BatchingConfig()
    }
  }

  class TestNodeBootstrap extends CantonNodeBootstrap[TestNode] {
    override def name: InstanceName = ???
    override def clock: Clock = ???
    override def crypto: Option[Crypto] = ???
    override def getId: Option[NodeId] = ???
    override def isInitialized: Boolean = ???
    override def start(): EitherT[Future, String, Unit] = EitherT.pure[Future, String](())
    override def getNode: Option[TestNode] = ???
    override def onClosed(): Unit = ()
    override protected def loggerFactory: NamedLoggerFactory = ???
    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override def isActive: Boolean = true
  }

  class TestNodeFactory {
    private class CreateResult(result: => TestNodeBootstrap) {
      def get = result
    }
    private val createResult = new AtomicReference[CreateResult](
      new CreateResult(new TestNodeBootstrap)
    )
    def setupCreate(result: => TestNodeBootstrap): Unit =
      createResult.set(new CreateResult(result))

    def create(name: String, config: TestNodeConfig): TestNodeBootstrap = createResult.get.get
  }

  class TestNodes(factory: TestNodeFactory, configs: Map[String, TestNodeConfig])
      extends ManagedNodes[TestNode, TestNodeConfig, CantonNodeParameters, TestNodeBootstrap](
        factory.create,
        new CommunityDbMigrationsFactory(loggerFactory),
        timeouts,
        configs,
        _ => MockedNodeParameters.cantonNodeParameters(),
        startUpGroup = 0,
        NodesTest.this.loggerFactory,
      ) {
    protected val executionContext: ExecutionContextIdlenessExecutorService =
      NodesTest.this.executorService
  }

  trait Fixture {
    val configs = Map(
      "n1" -> TestNodeConfig()
    )
    val nodeFactory = new TestNodeFactory
    val nodes = new TestNodes(nodeFactory, configs)
  }

  class StartStopFixture(startupResult: Either[String, Unit]) extends Fixture {
    val startPromise = Promise[Either[String, Unit]]()
    val startReached = Promise[Unit]()
    val node = new TestNodeBootstrap {
      override def start(): EitherT[Future, String, Unit] = {
        startReached.success(())
        EitherT(startPromise.future)
      }
    }
    nodeFactory.setupCreate(node)
    val start = nodes.start("n1")
    startReached.future.futureValue // wait until start happened
    val stop = nodes.stop("n1")
    // push start result
    startPromise.success(startupResult)
    // node should be properly closed and stop should succeed
    stop.value.futureValue shouldBe Right(())
    node.isClosing shouldBe true
    // wait for start to be have completed all callbacks including removing n1 from nodes.
    start.value.futureValue.discard
    nodes.isRunning("n1") shouldBe false
    startupResult match {
      case Left(value) => start.value.futureValue shouldBe Left(StartFailed("n1", value))
      case Right(_) => start.value.futureValue.isRight shouldBe true
    }

  }

  "starting a node" should {
    "return config not found error if using a bad id" in new Fixture {
      nodes.startAndWait("nope") shouldEqual Left(ConfigurationNotFound("nope"))
    }
    "not error if the node is already running when we try to start" in new Fixture {
      nodes.startAndWait("n1").map(_ => ()) shouldBe Right(()) // first create should work
      nodes.startAndWait("n1").map(_ => ()) shouldBe Right(()) // second is now a noop
    }
    "return an initialization failure if an exception is thrown during startup" in new Fixture {
      val exception = new RuntimeException("Nope!")
      nodeFactory.setupCreate { throw exception }
      the[RuntimeException] thrownBy Await.result(
        nodes.start("n1").value,
        10.seconds,
      ) shouldBe exception
    }
    "return a proper left if startup fails" in new Fixture {
      val node = new TestNodeBootstrap {
        override def start(): EitherT[Future, String, Unit] = EitherT.leftT("HelloBello")
      }
      nodeFactory.setupCreate(node)
      nodes.startAndWait("n1") shouldBe Left(StartFailed("n1", "HelloBello"))
      node.isClosing shouldBe true
    }
  }
  "stopping a node" should {
    "return config not found error if using a bad id" in new Fixture {
      nodes.stopAndWait("nope") shouldEqual Left(ConfigurationNotFound("nope"))
    }
    "return successfully if the node is not running" in new Fixture {
      nodes.stopAndWait("n1") shouldBe Right(())
    }
    "return an initialization failure if an exception is thrown during shutdown" in new Fixture {
      val anException = new RuntimeException("Nope!")
      val node = new TestNodeBootstrap {
        override def onClosed() = {
          throw anException
        }
      }
      nodeFactory.setupCreate(node)

      nodes.startAndWait("n1") shouldBe Right(())

      loggerFactory.assertThrowsAndLogs[ShutdownFailedException](
        nodes.stopAndWait("n1"),
        entry => {
          entry.warningMessage should fullyMatch regex "Closing .* failed! Reason:"
          entry.throwable.value shouldBe anException
        },
      )
    }
    "properly stop a running node" in new Fixture {
      nodes.startAndWait("n1") shouldBe Right(())
      nodes.isRunning("n1") shouldBe true
      nodes.stopAndWait("n1") shouldBe Right(())
      nodes.isRunning("n1") shouldBe false
    }
    "work when we are just starting" when {
      "start succeeded" in new StartStopFixture(Right(())) {}
      "start failed" in new StartStopFixture(Left("Stinky"))
    }
  }
}
