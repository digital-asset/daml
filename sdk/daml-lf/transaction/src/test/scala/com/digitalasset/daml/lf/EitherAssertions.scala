// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

trait EitherAssertions {
  self: org.scalatest.Assertions =>

  def assertRight[L, R](res: Either[L, R]): R = res match {
    case Left(err) => fail(s"Unexpected error: $err")
    case Right(x) => x
  }
}
