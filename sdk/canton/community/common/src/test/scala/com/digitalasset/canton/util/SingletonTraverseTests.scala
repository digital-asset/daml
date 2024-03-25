// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.kernel.CommutativeMonoid
import cats.laws.discipline.{TraverseTests, catsLawsIsEqToProp}
import cats.{CommutativeApplicative, Eq}
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Cogen, Prop}

trait SingletonTraverseTests[F[_], Context] extends TraverseTests[F] {
  override def laws: SingletonTraverseLaws[F, Context]

  def singletonTraverse[
      A: Arbitrary,
      B: Arbitrary,
      C: Arbitrary,
      M: Arbitrary,
      X[_]: CommutativeApplicative,
      Y[_]: CommutativeApplicative,
  ](implicit
      ArbFA: Arbitrary[F[A]],
      ArbFB: Arbitrary[F[B]],
      ArbXB: Arbitrary[X[B]],
      ArbXM: Arbitrary[X[M]],
      ArbYB: Arbitrary[Y[B]],
      ArbYC: Arbitrary[Y[C]],
      ArbYM: Arbitrary[Y[M]],
      ArbFXM: Arbitrary[F[X[M]]],
      CogenA: Cogen[A],
      CogenB: Cogen[B],
      CogenC: Cogen[C],
      CogenM: Cogen[M],
      M: CommutativeMonoid[M],
      MA: CommutativeMonoid[A],
      EqFA: Eq[F[A]],
      EqFC: Eq[F[C]],
      EqM: Eq[M],
      EqA: Eq[A],
      EqXYFC: Eq[X[Y[F[C]]]],
      EqXFB: Eq[X[F[B]]],
      EqYFB: Eq[Y[F[B]]],
      EqXFM: Eq[X[F[M]]],
      EqYFM: Eq[Y[F[M]]],
      EqOptionA: Eq[Option[A]],
      EqBoolean: Eq[Boolean],
      EqOptionC: Eq[Option[Context]],
  ): RuleSet = new RuleSet {
    def name: String = "SingletonTraverse"

    def bases: Seq[(String, RuleSet)] = Nil

    def parents: Seq[RuleSet] =
      Seq(traverse[A, B, C, M, X, Y])

    def props: Seq[(String, Prop)] =
      Seq(
        "size at most 1" -> forAll(laws.sizeAtMost1[A] _),
        "traverseSingleton consistency" -> forAll(laws.traverseSingletonConsistency[X, A, B] _),
        "context consistency" -> forAll(laws.contextConsistency[A] _),
      )
  }
}

object SingletonTraverseTests {
  def apply[F[_]](implicit FF: SingletonTraverse[F]): SingletonTraverseTests[F, FF.Context] =
    new SingletonTraverseTests[F, FF.Context] {
      override def laws: SingletonTraverseLaws[F, FF.Context] = SingletonTraverseLaws[F](FF)
    }
}
