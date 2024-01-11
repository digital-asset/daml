// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorGroupSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  object ErrorGroupBar extends ErrorGroup()(ErrorClass.root())

  object ErrorGroupFoo1 extends ErrorGroup()(ErrorClass.root()) {
    object ErrorGroupFoo2 extends ErrorGroup() {
      object ErrorGroupFoo3 extends ErrorGroup()
    }
  }

  it should "resolve correct error group names" in {
    ErrorGroupFoo1.ErrorGroupFoo2.ErrorGroupFoo3.errorClass shouldBe ErrorClass(
      List(
        Grouping("ErrorGroupFoo1", ErrorGroupFoo1.fullClassName),
        Grouping("ErrorGroupFoo2", ErrorGroupFoo1.ErrorGroupFoo2.fullClassName),
        Grouping(
          "ErrorGroupFoo3",
          ErrorGroupFoo1.ErrorGroupFoo2.ErrorGroupFoo3.fullClassName,
        ),
      )
    )
    ErrorGroupBar.errorClass shouldBe ErrorClass(
      List(Grouping("ErrorGroupBar", ErrorGroupBar.fullClassName))
    )
  }

}
