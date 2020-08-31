// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import com.daml.lf.data.Ref.Name

final case class Struct[+X] private (sortedFields: ImmArray[(Ref.Name, X)]) extends NoCopy {

  def lookup(name: Ref.Name): Option[X] =
    sortedFields.find(_._1 == name).map(_._2)

  def mapValue[Y](f: X => Y) = new Struct(sortedFields.map { case (k, v) => k -> f(v) })

  def toImmArray: ImmArray[(Ref.Name, X)] = sortedFields

  def names: Iterator[Ref.Name] = iterator.map(_._1)

  def values: Iterator[X] = iterator.map(_._2)

  def iterator: Iterator[(Ref.Name, X)] = sortedFields.iterator

  def foreach(f: ((Ref.Name, X)) => Unit): Unit = sortedFields.foreach(f)

}

object Struct {

  def apply[X](fields: (Name, X)*): Struct[X] =
    new Struct(fields.sortBy(_._1: String).to[ImmArray])

  def apply[X](fields: ImmArray[(Name, X)]): Struct[X] = apply(fields.toSeq: _*)

  def fromSortedImmArray[X](fields: ImmArray[(Ref.Name, X)]): Either[String, Struct[X]] = {
    val struct = new Struct(fields)
    Either.cond(
      (struct.names zip struct.names.drop(1)).forall { case (x, y) => (x compare y) <= 0 },
      struct,
      s"the list $fields is not sorted by name"
    )
  }

  private[this] val Emtpy = new Struct(ImmArray.empty)

  def empty[X]: Struct[X] = Emtpy.asInstanceOf[Struct[X]]

}
