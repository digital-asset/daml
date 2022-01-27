// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.time

import java.io.File
import java.lang
import java.util.concurrent.Callable
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.{IsStatusException, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.client.services.testing.time.StaticTime
import com.daml.platform.sandbox.fixture.SandboxFixture
import io.grpc.Status
import org.awaitility.Awaitility
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.tag._

import scala.concurrent.{Await, ExecutionContext}

class StaticTimeIT
    extends AnyWordSpec
    with SandboxFixture
    with SuiteResourceManagementAroundAll
    with ScalaFutures
    with Matchers {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)))

  implicit private def ec: ExecutionContext = materializer.executionContext

  private lazy val notLedgerId: domain.LedgerId = domain.LedgerId(s"not-${ledgerId().unwrap}")

  private val duration = java.time.Duration.ofSeconds(30L)

  override protected def packageFiles: List[File] = Nil

  def clientStub: TimeServiceStub = TimeServiceGrpc.stub(channel)

  "StaticTime" when {

    "reading time" should {

      "return it" in {
        withStaticTime(ledgerId())(_.getCurrentTime)
        succeed
      }
    }

    "updating time" should {

      "update its state when successfully setting time forward" in {
        withStaticTime(ledgerId()) { sut =>
          val newTime = sut.getCurrentTime.plus(duration)
          whenReady(sut.setTime(newTime)) { _ =>
            sut.getCurrentTime shouldEqual newTime
          }
        }
      }

      "synchronize time updates between different existing instances" in {
        withStaticTime(ledgerId()) { sut =>
          withStaticTime(ledgerId()) { sut2 =>
            assertTimeIsSynchronized(sut, sut2)
          }
        }
      }

      "initialize new instances with the latest time" in {
        withStaticTime(ledgerId()) { sut =>
          assertTimeIsSynchronized(sut, createStaticTime(ledgerId()))
        }
      }
    }

    "initialized with the wrong ledger ID" should {

      "fail during initialization" in {
        whenReady(StaticTime.updatedVia(clientStub, notLedgerId.unwrap).failed)(
          IsStatusException(Status.NOT_FOUND)
        )
      }
    }
  }

  private def assertTimeIsSynchronized(
      timeSetter: StaticTime,
      getTimeReader: => StaticTime,
  ): Assertion = {
    val oldTime = timeSetter.getCurrentTime
    val timeToSet = oldTime.plus(duration)

    whenReady(timeSetter.setTime(timeToSet)) { _ =>
      val readerInstance = getTimeReader
      try {
        val timeIsSynchronized = new Callable[lang.Boolean] {
          override def call(): lang.Boolean = {
            readerInstance.getCurrentTime == timeToSet
          }
        }

        Awaitility
          .await("Ledger Server time is synchronized to other StaticTime instance")
          .until(timeIsSynchronized)

        succeed
      } finally {
        readerInstance.close()
      }
    }
  }

  private def withStaticTime[T](ledgerId: domain.LedgerId)(action: StaticTime => T): T = {
    val staticTime = createStaticTime(ledgerId)
    val res = action(staticTime)
    staticTime.close()
    res
  }

  private def createStaticTime(ledgerId: domain.LedgerId) =
    Await.result(StaticTime.updatedVia(clientStub, ledgerId.unwrap), patienceConfig.timeout)
}
