// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

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
  // must not be a val
  def Instance: NewBoolean = new NewBoolean {
    type T = Boolean
    val False = false
    val True = true
    override def subst[F[_]](fa: F[T]) = fa
    override def apply(b: T) = b
  }

  // could almost `extends NewBoolean` but we preserve monomorphic calls this way
  abstract class Named {
    val NT: NewBoolean = Instance
    type T = NT.T
    protected val False: T = NT.False
    protected val True: T = NT.True
    def subst[F[_]](fa: F[Boolean]): F[T] = NT subst fa
    def apply(b: Boolean): T = NT(b)
  }
}
