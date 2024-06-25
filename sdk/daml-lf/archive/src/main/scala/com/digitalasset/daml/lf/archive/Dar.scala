// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import scalaz.{Applicative, Equal, Traverse}
import scalaz.syntax.equal._
import scalaz.std.list._

final case class Dar[A](main: A, dependencies: List[A]) {
  lazy val all: List[A] = main :: dependencies
}

object Dar {
  implicit val darTraverse: Traverse[Dar] = new Traverse[Dar] {
    override def map[A, B](fa: Dar[A])(f: A => B): Dar[B] =
      Dar[B](main = f(fa.main), dependencies = fa.dependencies.map(f))

    override def traverseImpl[G[_]: Applicative, A, B](fa: Dar[A])(f: A => G[B]): G[Dar[B]] = {
      import scalaz.syntax.apply._
      import scalaz.syntax.traverse._
      import scalaz.std.list._
      val gb: G[B] = f(fa.main)
      val gbs: G[List[B]] = fa.dependencies.traverse(f)
      ^(gb, gbs)((b, bs) => Dar(b, bs))
    }
  }

  implicit def darEqual[A: Equal]: Equal[Dar[A]] = new Equal[Dar[A]] {
    override def equal(a1: Dar[A], a2: Dar[A]): Boolean =
      a1.main === a2.main && a1.dependencies === a2.dependencies
  }
}
