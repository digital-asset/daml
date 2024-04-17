// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.Monad
import org.scalacheck.Gen

object GenInstances {
  implicit val genMonad: Monad[Gen] = new Monad[Gen] {

    override def flatMap[A, B](fa: Gen[A])(f: A => Gen[B]): Gen[B] = fa.flatMap(f)

    // not tail recursive ¯\_(ツ)_/¯
    override def tailRecM[A, B](a: A)(f: A => Gen[Either[A, B]]): Gen[B] =
      f(a).flatMap {
        case Left(k) => tailRecM(k)(f)
        case Right(x) => Gen.const(x)
      }

    override def pure[A](x: A): Gen[A] = Gen.const(x)
  }
}
