// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.DottedName

private[validation] object Util {

  implicit final class TupleImmArrayOps[A, B](val array: ImmArray[(A, B)]) extends AnyVal {
    def unzip: (ImmArray[A], ImmArray[B]) = {
      val (a1, a2) = array.toSeq.unzip
      (ImmArray(a1), ImmArray(a2))
    }

    def keys: Iterator[A] = array.iterator.map(_._1)

    def values: Iterator[B] = array.iterator.map(_._2)

    def mapValues[C](f: B => C): ImmArray[(A, C)] = array.map { case (k, v) => k -> f(v) }

    def toMap: Map[A, B] = array.iterator.toMap

    def lookup(key: A, e: => ValidationError): B = array.find(_._1 == key).fold(throw e)(_._2)
  }

  implicit final class DottedNameOps(val name: DottedName) extends AnyVal {
    def ++(other: DottedName): DottedName =
      DottedName.unsafeFromSegments(name.segments.slowAppend(other.segments))

    def +(id: String): DottedName =
      DottedName.assertFromSegments(name.segments.slowSnoc(id))

    def toUpperCase: DottedName =
      DottedName.unsafeFromSegments(name.segments.map(_.toUpperCase))
  }

}
