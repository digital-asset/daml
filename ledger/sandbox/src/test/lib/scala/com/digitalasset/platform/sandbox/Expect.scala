// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import org.scalatest.{AsyncTestSuite, Matchers}

import scala.concurrent.Future

trait Expect { self: AsyncTestSuite with Matchers =>

  def expect[T](f: => Future[T]) = new Expectation(f, self)

}
