// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import io.grpc.Status
import org.scalatest.{Assertion, AsyncTestSuite, Matchers}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final class Expectation[T](f: Future[T], suite: AsyncTestSuite with Matchers) {

  import suite._

  lazy val toSucceed: Future[Assertion] = f.map(_ => succeed)

  lazy val toBeDenied: Future[Assertion] = toFailWith(Status.Code.PERMISSION_DENIED)

  def toFailWith(code: Status.Code): Future[Assertion] =
    f.transform {
      case Failure(GrpcException(GrpcStatus(`code`, _), _)) => Try(succeed)
      case Failure(NonFatal(e)) => Try(fail(e))
      case Success(_) => Try(fail(s"Expected failure ${code.name()}"))
    }

}
