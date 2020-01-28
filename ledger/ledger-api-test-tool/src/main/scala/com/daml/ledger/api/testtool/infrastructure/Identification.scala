// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

object Identification {

  val greekAlphabet = Vector(
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
  )

  val latinAlphabet = Vector('a'.to('z'): _*).map(_.toString)

  /**
    * E.g.
    *
    * val ids = circularWithIndex(Vector("a", "b", "c"))
    *
    * assert(ids() == "a")
    * assert(ids() == "b")
    * assert(ids() == "c")
    * assert(ids() == "a0")
    * assert(ids() == "b0")
    * assert(ids() == "c0")
    * assert(ids() == "a1")
    */
  def circularWithIndex(base: Vector[String]): () => String =
    synchronizedProvider(base.iterator ++ Iterator.continually(base).zipWithIndex.flatMap {
      case (alphabet, index) => alphabet.map(letter => s"$letter$index")
    })

  /**
    * E.g.
    *
    * val ids = indexSuffix("prefix")
    *
    * assert(ids() == "prefix-0")
    * assert(ids() == "prefix-1")
    * assert(ids() == "prefix-2")
    */
  def indexSuffix(template: String): () => String =
    synchronizedProvider(Iterator.from(0).map(n => s"$template-$n"))

  /**
    * Rules out race conditions when accessing an iterator
    */
  private def synchronizedProvider[A](it: Iterator[A]): () => A =
    () => it.synchronized(it.next())

}
