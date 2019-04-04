// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick.implicits

import com.digitalasset.ods.slick.SlickProfile
import org.scalacheck.Arbitrary

@SuppressWarnings(Array("org.wartremover.warts.Any"))
trait Wrappers { self: SlickProfile =>
  import self.profile.api._

  private val internalWrapperList = new collection.mutable.MutableList[Wrapper[_]]

  def wrapper[T: ColumnType](columnName: String)(implicit tArb: Arbitrary[T]): Wrapper[T] = {
    val wrapper = new Wrapper[T](columnName)(implicitly[ColumnType[T]], tArb)
    internalWrapperList += wrapper
    wrapper
  }

  def wrappers: List[Wrapper[_]] = internalWrapperList.toList

  class Wrapper[T: ColumnType](val columnName: String)(implicit tArb: Arbitrary[T]) {

    // RowClass is sealed instead of final due to: https://issues.scala-lang.org/browse/SI-4440
    sealed case class RowClass(t: T)

    class Table(tag: Tag) extends profile.api.Table[RowClass](tag, columnName + "_table") {
      def c = column[T](columnName)
      def * = c <> (RowClass.apply, RowClass.unapply)
    }

    val query = TableQuery[Table]

    implicit val rowClassArbitrary: Arbitrary[RowClass] =
      Arbitrary[RowClass](tArb.arbitrary.map(RowClass.apply))
  }
}
