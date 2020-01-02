// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import language.higherKinds

/** {{{
  *  object IsTyped extends NewBoolean.Named {
  *    val SinglyTyped = False
  *    val WellTyped = True
  *  }
  *
  *  List[IsTyped.T](IsTyped.SinglyTyped, IsTyped.WellTyped)
  * }}}
  */
sealed abstract class NewBoolean {
  type T <: Boolean
  val False: T
  val True: T
  def subst[F[_]](fa: F[Boolean]): F[T]
  def apply(b: Boolean): T
}

object NewBoolean {
  def Instance: NewBoolean = new NewBoolean {
    type T = Boolean
    val False = false
    val True = true
    override def subst[F[_]](fa: F[T]) = fa
    override def apply(b: T) = b
  }

  // technically could `extends NewBoolean` but we preserve monomorphic calls this way
  abstract class Named {
    val NT: NewBoolean = Instance
    type T = NT.T
    val False: T = NT.False
    val True: T = NT.True
    def subst[F[_]](fa: F[Boolean]): F[T] = NT subst fa
    def apply(b: Boolean): T = NT(b)
  }
}
