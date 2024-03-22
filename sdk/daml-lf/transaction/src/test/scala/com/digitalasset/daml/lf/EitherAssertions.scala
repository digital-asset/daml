// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

trait EitherAssertions {
  self: org.scalatest.Assertions =>

  def assertRight[L, R](res: Either[L, R]): R = res match {
    case Left(err) => fail(s"Unexpected error: $err")
    case Right(x) => x
  }
}
