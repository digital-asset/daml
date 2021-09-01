// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import scalaz.{@@, Order, Semigroup, Tag, Tags}

/** [[Tags]] that can stack onto another tag.
  * (scalaz 7.3 tags stack, 7.2 tags do not)
  */
private[http] sealed abstract class XTag {
  type T[+A, X]
  // a proper subst's F is F[_[+_, _]], so this is "subst0" instead
  def subst0[F[_], A, X](fa: F[A]): F[T[A, X]]
}

private[http] object XTag {
  type XX[+A, X] = Instance.T[A, X]

  val Instance: XTag = new XTag {
    type T[+A, X] = A
    override def subst0[F[_], A, X](fa: F[A]) = fa
  }

  // why is stacking tags ok here? because in the body, A's tag,
  // if any, is abstracted away
  implicit def `MaxVal instance`[A: Order]: Semigroup[A XX Tags.MaxVal] =
    Instance.subst0(Tag.unsubst(implicitly[Semigroup[A @@ Tags.MaxVal]]))
}
