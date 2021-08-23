// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.io.PrintWriter
import java.nio.file.Path

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.LedgerInitialConditions
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v1.Update
import com.daml.lf.data.Time
import org.mockito.ArgumentMatchers.anyString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.ExecutionContext

class StateUpdateExporterSpec
    extends TestKit(ActorSystem(classOf[StateUpdateExporterSpec].getSimpleName))
    with AsyncWordSpecLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterAll {
  import StateUpdateExporterSpec._

  private[this] implicit val materializer: Materializer = Materializer(system)
  private[this] implicit val ec: ExecutionContext = materializer.executionContext

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "write" should {
    "write updates" in {
      val expectedUpdatesPath = mock[Path]
      val actualUpdatesPath = mock[Path]
      val expectedUpdatesWriter = mock[PrintWriter]
      val actualUpdatesWriter = mock[PrintWriter]
      val expectedRegex = "PublicPackageUpload\\(.*\\)"
      StateUpdateExporter
        .write(
          aReadService,
          aReadService,
          Map(
            expectedUpdatesPath -> expectedUpdatesWriter,
            actualUpdatesPath -> actualUpdatesWriter,
          ),
          aConfig(Some(expectedUpdatesPath), Some(actualUpdatesPath)),
        )
        .map { _ =>
          verify(expectedUpdatesWriter).println(matches(expectedRegex))
          verify(actualUpdatesWriter).println(matches(expectedRegex))
          succeed
        }
    }

    "not write anything if the config parameters were not provided" in {
      val mockWriter = mock[PrintWriter]
      StateUpdateExporter
        .write(
          aReadService,
          aReadService,
          _ => mockWriter,
          aConfig(),
        )
        .map { _ =>
          verify(mockWriter, never).write(anyString())
          succeed
        }
    }
  }
}

object StateUpdateExporterSpec extends MockitoSugar {
  private val aReadService = new ReplayingReadService {
    override def updateCount(): Long = 1

    override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
      Source.empty

    override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
      Source.single(
        (
          Offset.beforeBegin,
          Update.PublicPackageUpload(
            List.empty,
            None,
            Time.Timestamp.MinValue,
            None,
          ),
        )
      )

    override def currentHealth(): HealthStatus = Healthy
  }

  private def aConfig(
      expectedUpdatesPath: Option[Path] = None,
      actualUpdatesPath: Option[Path] = None,
  ) = Config(
    mock[Path],
    performByteComparison = false,
    sortWriteSet = false,
    expectedUpdatesPath = expectedUpdatesPath,
    actualUpdatesPath = actualUpdatesPath,
  )
}
