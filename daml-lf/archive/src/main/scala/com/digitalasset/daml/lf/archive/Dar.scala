// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import scalaz.Functor

case class Dar[A](main: A, dependencies: List[A]) {
  lazy val all: List[A] = main :: dependencies
}

object Dar {
  implicit val darFunctor: Functor[Dar] = new Functor[Dar] {
    override def map[A, B](fa: Dar[A])(f: A => B): Dar[B] =
      Dar[B](main = f(fa.main), dependencies = fa.dependencies.map(f))
  }
}
