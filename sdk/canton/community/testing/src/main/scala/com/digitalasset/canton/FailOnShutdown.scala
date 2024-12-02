// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import org.scalactic.source.Position
import org.scalatest.Assertion
import org.scalatest.Assertions.fail

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/** Trait to automatically convert FutureUnlessShutdown to test failures in case of shutdown. */
trait FailOnShutdown {

  implicit def convertFus2Future(
      fus: FutureUnlessShutdown[Assertion]
  )(implicit ec: ExecutionContext, pos: Position): Future[Assertion] =
    fus.onShutdown(fail("Unexpected shutdown"))

  implicit def convertEitherFus2Future[A](
      EitherTFus: EitherT[FutureUnlessShutdown, A, Assertion]
  )(implicit ec: ExecutionContext, pos: Position): Future[Assertion] =
    EitherTFus.onShutdown(fail("Unexpected shutdown")).value.map {
      case Left(a) => fail(a.toString)
      case Right(b) => b
    }
}

object FailOnShutdown extends FailOnShutdown
