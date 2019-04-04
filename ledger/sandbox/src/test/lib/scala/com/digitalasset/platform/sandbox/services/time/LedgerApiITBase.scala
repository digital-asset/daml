// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.time

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait LedgerApiITBase extends WordSpecLike with ScalaFutures with Matchers with OptionValues {

  protected def ledgerId: String

  protected lazy val notLedgerId: String = "not " + ledgerId

  protected val timeoutSeconds = 30L

  override implicit def patienceConfig: PatienceConfig =
    super.patienceConfig.copy(timeout = Span(timeoutSeconds, Seconds))

  protected val timeoutDuration: FiniteDuration = timeoutSeconds.seconds

  def await[T](f: Future[T]): T = Await.result(f, timeoutDuration)
}
