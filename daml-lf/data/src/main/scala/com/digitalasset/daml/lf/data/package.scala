// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

package object data {

  def assertRight[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

  val Numeric: NumericModule = new NumericModule {
    override type Numeric = java.math.BigDecimal
    @inline
    override private[data] def cast(x: java.math.BigDecimal): Numeric = x
  }
  type Numeric = Numeric.Numeric

  trait NoCopy {
    // prevents autogeneration of copy method in case class
    protected def copy(nothing: Nothing): Nothing = nothing
  }

}
