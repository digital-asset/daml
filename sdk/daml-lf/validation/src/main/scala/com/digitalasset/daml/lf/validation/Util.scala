// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref
import com.daml.lf.language

private[validation] object Util {

  implicit final class TupleImmArrayOps[A, B](val array: ImmArray[(A, B)]) extends AnyVal {
    def unzip: (ImmArray[A], ImmArray[B]) = {
      val (a1, a2) = array.toSeq.unzip
      (a1.toImmArray, a2.toImmArray)
    }

    def keys: Iterator[A] = array.iterator.map(_._1)

    def values: Iterator[B] = array.iterator.map(_._2)

    def transform[C](f: (A, B) => C): ImmArray[(A, C)] = array.map { case (k, v) => k -> f(k, v) }

    def toMap: Map[A, B] = array.toSeq.toMap

    def lookup(key: A, e: => ValidationError): B = array.find(_._1 == key).fold(throw e)(_._2)
  }

  implicit final class DottedNameOps(val name: Ref.DottedName) extends AnyVal {
    def ++(other: Ref.DottedName): Ref.DottedName =
      Ref.DottedName.unsafeFromNames(name.segments.slowAppend(other.segments))

    def +(id: Ref.Name): Ref.DottedName =
      Ref.DottedName.unsafeFromNames(name.segments.slowSnoc(id))

    def toUpperCase: Ref.DottedName =
      Ref.DottedName.unsafeFromNames(
        name.segments.map(i => Ref.Name.assertFromString(i.toUpperCase))
      )
  }

  private[validation] def handleLookup[X](ctx: => Context, x: Either[language.LookupError, X]) =
    x match {
      case Right(value) =>
        value
      case Left(err) =>
        throw EUnknownDefinition(ctx, err)
    }

}
