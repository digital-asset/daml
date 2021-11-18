// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorGroupSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  object ErrorGroupBar extends ErrorGroup()(ErrorClass.root())

  object ErrorGroupsFoo {
    private implicit val errorClass: ErrorClass = ErrorClass.root()

    object ErrorGroupFoo1 extends ErrorGroup() {
      object ErrorGroupFoo2 extends ErrorGroup() {
        object ErrorGroupFoo3 extends ErrorGroup()
      }
    }
  }

  it should "resolve correct error group names" in {
    ErrorGroupsFoo.ErrorGroupFoo1.ErrorGroupFoo2.ErrorGroupFoo3.errorClass shouldBe ErrorClass(
      List(
        Grouping("ErrorGroupFoo1", Some(ErrorGroupsFoo.ErrorGroupFoo1)),
        Grouping("ErrorGroupFoo2", Some(ErrorGroupsFoo.ErrorGroupFoo1.ErrorGroupFoo2)),
        Grouping(
          "ErrorGroupFoo3",
          Some(ErrorGroupsFoo.ErrorGroupFoo1.ErrorGroupFoo2.ErrorGroupFoo3),
        ),
      )
    )
    ErrorGroupBar.errorClass shouldBe ErrorClass(
      List(Grouping("ErrorGroupBar", Some(ErrorGroupBar)))
    )
  }

}
