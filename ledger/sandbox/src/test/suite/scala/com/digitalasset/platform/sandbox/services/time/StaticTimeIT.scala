// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.time

import java.io.File
import java.lang
import java.util.concurrent.Callable

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, IsStatusException, SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.sandbox.TestExecutionSequencerFactory
import com.digitalasset.platform.sandbox.services.SandboxFixture
import io.grpc.Status
import org.awaitility.Awaitility
import org.scalatest.{Matchers, WordSpec}

import scalaz.syntax.tag._
import scala.concurrent.Await
import scala.concurrent.duration._

class StaticTimeIT
    extends WordSpec
    with LedgerApiITBase
    with SandboxFixture
    with AkkaBeforeAndAfterAll
    with TestExecutionSequencerFactory
    with SuiteResourceManagementAroundAll
    with Matchers {

  implicit private def ec = materializer.executionContext

  private val duration = java.time.Duration.ofSeconds(30L)

  override protected def packageFiles: List[File] = Nil

  def clientStub = TimeServiceGrpc.stub(channel)

  "StaticTime" when {

    "reading time" should {

      "return it" in {
        withStaticTime(ledgerId)(_.getCurrentTime)
        succeed
      }
    }

    "updating time" should {

      "update its state when successfully setting time forward" in {
        withStaticTime(ledgerId) { sut =>
          val newTime = sut.getCurrentTime.plus(duration)
          whenReady(sut.setTime(newTime)) { _ =>
            sut.getCurrentTime shouldEqual newTime
          }
        }
      }

      "synchronize time updates between different existing instances" in {
        withStaticTime(ledgerId) { sut =>
          withStaticTime(ledgerId) { sut2 =>
            assertTimeIsSynchronized(sut, sut2)
          }
        }
      }

      "initialize new instances with the latest time" in {
        withStaticTime(ledgerId) { sut =>
          assertTimeIsSynchronized(sut, createStaticTime(ledgerId))
        }
      }
    }

    "initialized with the wrong ledger ID" should {

      "fail during initialization" in {
        whenReady(StaticTime.updatedVia(clientStub, notLedgerId.unwrap).failed)(
          IsStatusException(Status.NOT_FOUND))
      }
    }
  }

  private def assertTimeIsSynchronized(timeSetter: StaticTime, getTimeReader: => StaticTime) = {
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

  private def withStaticTime[T](ledgerId: domain.LedgerId)(action: StaticTime => T) = {
    val staticTime = createStaticTime(ledgerId)
    val res = action(staticTime)
    staticTime.close()
    res
  }

  private def createStaticTime(ledgerId: domain.LedgerId) = {
    Await.result(StaticTime.updatedVia(clientStub, ledgerId.unwrap), 30.seconds)
  }
}
