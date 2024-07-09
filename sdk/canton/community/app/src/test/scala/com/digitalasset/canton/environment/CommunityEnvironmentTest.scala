// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{CantonCommunityConfig, TestingConfigInternal}
import com.digitalasset.canton.domain.DomainNodeBootstrap
import com.digitalasset.canton.domain.config.{
  CommunityDomainConfig,
  CommunityPublicServerConfig,
  DomainConfig,
}
import com.digitalasset.canton.integration.CommunityConfigTransforms
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.participant.{ParticipantNode, ParticipantNodeBootstrap}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{BaseTest, ConfigStubs, HasExecutionContext}
import monocle.macros.syntax.lens.*
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class CommunityEnvironmentTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  // we don't care about any values of this config, so just mock
  lazy val domain1Config: CommunityDomainConfig = ConfigStubs.domain
  lazy val domain2Config: CommunityDomainConfig = ConfigStubs.domain
  lazy val participant1Config: CommunityParticipantConfig = ConfigStubs.participant
  lazy val participant2Config: CommunityParticipantConfig = ConfigStubs.participant

  lazy val sampleConfig: CantonCommunityConfig = CantonCommunityConfig(
    domains = Map(
      InstanceName.tryCreate("d1") -> domain1Config,
      InstanceName.tryCreate("d2") -> domain2Config,
    ),
    participants = Map(
      InstanceName.tryCreate("p1") -> participant1Config,
      InstanceName.tryCreate("p2") -> participant2Config,
    ),
  )

  trait CallResult[A] {
    def get: A
  }

  trait TestEnvironment {
    def config: CantonCommunityConfig = sampleConfig

    private val createParticipantMock =
      mock[(String, LocalParticipantConfig) => ParticipantNodeBootstrap]
    private val createDomainMock = mock[(String, DomainConfig) => DomainNodeBootstrap]

    def mockDomain: DomainNodeBootstrap = {
      val domain = mock[DomainNodeBootstrap]
      when(domain.start()).thenReturn(EitherT.pure[Future, String](()))
      when(domain.name).thenReturn(InstanceName.tryCreate("mockD"))
      domain
    }

    def mockParticipantAndNode: (ParticipantNodeBootstrap, ParticipantNode) = {
      val bootstrap = mock[ParticipantNodeBootstrap]
      val node = mock[ParticipantNode]
      when(bootstrap.name).thenReturn(InstanceName.tryCreate("mockP"))
      when(bootstrap.start()).thenReturn(EitherT.pure[Future, String](()))
      when(bootstrap.getNode).thenReturn(Some(node))
      when(node.reconnectDomainsIgnoreFailures()(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.pure[FutureUnlessShutdown, SyncServiceError](()))
      when(node.config).thenReturn(participant1Config)
      (bootstrap, node)
    }
    def mockParticipant: ParticipantNodeBootstrap = mockParticipantAndNode._1

    val environment = new CommunityEnvironment(
      config,
      TestingConfigInternal(initializeGlobalOpenTelemetry = false),
      loggerFactory,
    ) {
      override def createParticipant(
          name: String,
          participantConfig: CommunityParticipantConfig,
      ): ParticipantNodeBootstrap =
        createParticipantMock(name, participantConfig)
      override def createDomain(
          name: String,
          domainConfig: CommunityDomainConfig,
      ): DomainNodeBootstrap =
        createDomainMock(name, domainConfig)
    }

    protected def setupParticipantFactory(create: => ParticipantNodeBootstrap): Unit =
      setupParticipantFactoryInternal(anyString(), create)

    protected def setupParticipantFactory(id: String, create: => ParticipantNodeBootstrap): Unit =
      setupParticipantFactoryInternal(ArgumentMatchers.eq(id), create)

    private def setupParticipantFactoryInternal(
        idMatcher: => String,
        create: => ParticipantNodeBootstrap,
    ): Unit =
      when(createParticipantMock(idMatcher, any[LocalParticipantConfig])).thenAnswer(create)

    protected def setupDomainFactory(id: String, create: => DomainNodeBootstrap): Unit =
      when(createDomainMock(eqTo(id), any[DomainConfig])).thenAnswer(create)
  }

  "Environment" when {
    "starting with startAndReconnect" should {
      "succeed normally" in new TestEnvironment {

        val pp = mockParticipant
        Seq("p1", "p2").foreach(setupParticipantFactory(_, pp))
        Seq("d1", "d2").foreach(setupDomainFactory(_, mockDomain))

        environment.startAndReconnect(false) shouldBe Right(())
        verify(pp.getNode.valueOrFail("node should be set"), times(2))
          .reconnectDomainsIgnoreFailures()(any[TraceContext], any[ExecutionContext])

      }

      "auto-connect if requested" in new TestEnvironment {

        override def config: CantonCommunityConfig =
          (CommunityConfigTransforms.updateAllDomainConfigs { case (_, config) =>
            config
              .focus(_.publicApi)
              .replace(CommunityPublicServerConfig(internalPort = Some(Port.tryCreate(42))))
          })(sampleConfig)

        val (pp, pn) = mockParticipantAndNode
        val d1 = mockDomain
        val d2 = mockDomain

        when(pp.isActive).thenReturn(true)
        when(d1.isActive).thenReturn(true)
        when(d2.isActive).thenReturn(false)

        when(d1.config).thenReturn(
          config.domainsByString.get("d1").valueOrFail("where is my config?")
        )
        when(
          pn.autoConnectLocalDomain(any[DomainConnectionConfig])(
            any[TraceContext],
            any[ExecutionContext],
          )
        ).thenReturn(EitherTUtil.unitUS)

        Seq("p1", "p2").foreach(setupParticipantFactory(_, pp))
        setupDomainFactory("d1", d1)
        setupDomainFactory("d2", d2)

        clue("auto-start") {
          environment.startAndReconnect(true) shouldBe Right(())
        }

        verify(pn, times(2)).autoConnectLocalDomain(any[DomainConnectionConfig])(
          any[TraceContext],
          any[ExecutionContext],
        )

      }

      "write ports file if desired" in new TestEnvironment {

        override def config: CantonCommunityConfig = {
          val tmp = sampleConfig.focus(_.parameters.portsFile).replace(Some("my-ports.txt"))
          (CommunityConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
            config
              .focus(_.ledgerApi)
              .replace(LedgerApiServerConfig(internalPort = Some(Port.tryCreate(42))))
          })(tmp)
        }

        val f = new java.io.File("my-ports.txt")
        f.deleteOnExit()

        val pp = mockParticipant
        when(pp.config).thenReturn(
          config.participantsByString.get("p1").valueOrFail("config should be there")
        )
        Seq("p1", "p2").foreach(setupParticipantFactory(_, pp))
        Seq("d1", "d2").foreach(setupDomainFactory(_, mockDomain))

        clue("write ports file") {
          environment.startAndReconnect(false) shouldBe Right(())
        }
        assert(f.exists())

      }

      "not start if manual start is desired" in new TestEnvironment {
        override def config: CantonCommunityConfig =
          sampleConfig.focus(_.parameters.manualStart).replace(true)

        // These would throw on start, as all methods return null.
        val myDomain: DomainNodeBootstrap = mock[DomainNodeBootstrap]
        val myParticipant: ParticipantNodeBootstrap = mock[ParticipantNodeBootstrap]

        Seq("p1", "p2").foreach(setupParticipantFactory(_, myParticipant))
        Seq("d1", "d2").foreach(setupDomainFactory(_, myDomain))

        environment.startAndReconnect(false) shouldBe Right(())
      }

      "report exceptions" in new TestEnvironment {
        val exception = new RuntimeException("wurstsalat")

        Seq("p1", "p2").foreach(setupParticipantFactory(_, throw exception))
        Seq("d1", "d2").foreach(setupDomainFactory(_, throw exception))

        assertThrows[RuntimeException](environment.startAndReconnect(false))

      }
    }
    "starting with startAll" should {
      "report exceptions" in new TestEnvironment {
        val exception = new RuntimeException("nope")

        // p1, d1 and d2 will successfully come up
        val d1: DomainNodeBootstrap = mockDomain
        val d2: DomainNodeBootstrap = mockDomain
        val p1: ParticipantNodeBootstrap = mockParticipant
        setupParticipantFactory("p1", p1)
        setupDomainFactory("d1", d1)
        setupDomainFactory("d2", d2)

        // p2 will fail to come up
        setupParticipantFactory("p2", throw exception)
        the[RuntimeException] thrownBy environment.startAll() shouldBe exception
        // start all will kick off stuff in the background but the "parTraverseWithLimit"
        // will terminate eagerly. so we actually have to wait until the processes finished
        // in the background
        eventually() {
          environment.domains.running.toSet shouldBe Set(d1, d2)
          environment.participants.running should contain.only(p1)
        }
      }
    }
  }

}
