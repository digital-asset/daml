// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scala.util

import org.scalatest.{FlatSpec, Matchers}

class StatementSpec extends FlatSpec with Matchers {
  import com.daml.scalatest.Equalz._
  import scalaz.std.anyVal._

  behavior of Statement.getClass.getSimpleName

  it should "evaluate passed expression for side effects" in {
    var counter: Int = 0

    def increment(): Int = {
      counter += 1
      counter
    }

    Statement.discard(increment()): Unit

    Statement.discard {
      counter += 1
    }: Unit

    counter shouldx equalz(2)
  }

  it should "not evaluate passed lambda expression" in {
    var counter: Int = 0

    Statement.discard { () =>
      counter += 1
    }

    counter shouldx equalz(0)
  }
}
