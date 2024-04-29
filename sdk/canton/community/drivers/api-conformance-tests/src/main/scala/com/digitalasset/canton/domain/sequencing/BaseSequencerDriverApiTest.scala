// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.block.SequencerDriver
import com.digitalasset.canton.domain.sequencing.BaseSequencerDriverApiTest.CompletionTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait BaseSequencerDriverApiTest[ConfigType]
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfterAll
    with HasExecutionContext {

  private val timeProvider: TimeProvider = new TimeProvider {
    override def nowInMicrosecondsSinceEpoch: Long = Instant.now().toEpochMilli
  }

  protected final val loggerFactory: NamedLoggerFactory =
    NamedLoggerFactory.unnamedKey("test", getClass.getSimpleName)

  protected final implicit def traceContext: TraceContext =
    W3CTraceContext("00-5c14649f3f82e93b658a1f34e5f6aece-93bb0fa23a8fb53a-01").toTraceContext

  implicit final protected val actorSystem: ActorSystem =
    ActorSystem(
      name = "sequencer-driver-conformance-tests-actor-system",
      defaultExecutionContext = Some(parallelExecutionContext),
      config = Some(ConfigFactory.load),
    )

  protected final def domainId: DomainId = DefaultTestIdentities.domainId

  protected final def mediatorId: MediatorId = DefaultTestIdentities.daMediator

  protected final def topologyClientMember: Member = DefaultTestIdentities.daSequencerId

  private val topologyFactory =
    new TestingIdentityFactory(
      topology = TestingTopology(),
      loggerFactory,
      List.empty,
    )
  private val topologyClient =
    topologyFactory.forOwnerAndDomain(owner = mediatorId, domainId)

  protected val driverConfig: AtomicReference[Option[ConfigType]]

  override def afterAll(): Unit = {
    val _ = Await.result(actorSystem.terminate(), CompletionTimeout)
    super.afterAll()
  }

  // Driver instances may need to be created per-test, as drivers don't have to support subsequent subscriptions.
  // Driver instantiation also provides a way to test restarts at the driver API level.
  protected def createDriver(
      timeProvider: TimeProvider = timeProvider,
      firstBlockHeight: Option[Long] = None,
      topologyClient: DomainSyncCryptoClient = topologyClient,
  ): SequencerDriver
}

object BaseSequencerDriverApiTest {

  val CompletionTimeout: FiniteDuration = 10.seconds
}
