// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.language.implicitConversions
import scalaz.{\/, IsCovariant}
import scalaz.Liskov.<~<

private[daml] object WidenEither {
  implicit final class `widenLeft syntax`[B[_, _], L, R](private val self: L B R) extends AnyVal {
    def widenLeft[U](implicit st: L <~< U, ev: IsCovariant[* B R]): U B R = ev widen self
    def widenRight[U](implicit st: R <~< U, ev: IsCovariant[L B *]): L B U = ev widen self
    def biwiden[UL, UR](implicit
        ul: L <~< UL,
        ur: R <~< UR,
        evl: IsCovariant[* B R],
        evr: IsCovariant[UL B *],
    ): UL B UR = evr widen (evl widen self)
  }

  implicit final class `widenLeftF syntax`[F[_], B[_, _], L, R](private val self: F[L B R])
      extends AnyVal {
    def widenLeftF[U](implicit st: L <~< U, ev: IsCovariant[* B R], F: IsCovariant[F]): F[U B R] =
      (F liftLiskovCo (ev liftLiskovCo st)).apply(self)
    def biwidenF[UL, UR](implicit
        ul: L <~< UL,
        ur: R <~< UR,
        evl: IsCovariant[* B R],
        evr: IsCovariant[UL B *],
        F: IsCovariant[F],
    ): F[UL B UR] = F.liftLiskovCo(evl.liftLiskovCo(ul) andThen evr.liftLiskovCo(ur)).apply(self)
  }

  object Conversions {
    // XXX deprecate to point out all locations that need ascription
    // @deprecated("widen the right and left", since = "1.14.0")
    implicit def `widen either for scalaz 73`[L1, R1, L2, R2](e: L1 \/ R1): L2 \/ R2 =
      e.biwiden
  }
}
