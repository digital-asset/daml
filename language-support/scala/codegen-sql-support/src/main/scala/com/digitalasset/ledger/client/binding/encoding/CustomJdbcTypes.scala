// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import java.sql.{PreparedStatement, ResultSet}
import java.util.{Calendar, TimeZone}

import SlickTypeEncoding.SupportedProfile
import com.digitalasset.ledger.client.binding.{Primitive => P}

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
  * @tparam T   DB column type to store `P.List` values
  * @tparam F   Typeclass, for the format to which `P.List` gets converted/serialized before saving. E.g. JsonFormat
  */
class CustomJdbcTypes[T, F[_], +Profile <: SupportedProfile] private (
    val profile: Profile,
    listCodec: ListOptionMapCodec[T, F])(implicit T: Profile#ColumnType[T]) {
  import SqlTypeConverters._
  import profile.{ColumnType, BaseColumnType}
  import profile.api._

  // profile.columnTypes.nullJdbcType seems appropriate here,
  // but emits type 'NULL NOT NULL' in create table
  val primitiveUnitColumnType: BaseColumnType[P.Unit] =
    profile.MappedColumnType.base[Unit, Boolean](_ => false, _ => ())

  implicit val `primitive date classTag`: ClassTag[P.Date] =
    P.Date.subst(implicitly[ClassTag[java.time.LocalDate]])

  val primitiveDateColumnType: BaseColumnType[P.Date] =
    profile.MappedColumnType.base(primitiveDateToSqlDate, sqlDateToPrimitiveDate)

  implicit val `primitive time classTag`: ClassTag[P.Timestamp] =
    P.Timestamp.subst(implicitly[ClassTag[java.time.Instant]])

  // the default timezone column as provided by jdbc takes the calendar using the current
  // timezone, which causes issues since (for example) it handles daylight saving while
  // we just want to store a dumb epoch timestamp.
  val primitiveTimestampColumnType: BaseColumnType[P.Timestamp] =
    new profile.DriverJdbcType[P.Timestamp] {
      val utcCalendar: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

      def sqlType: Int = java.sql.Types.TIMESTAMP
      def setValue(v: P.Timestamp, p: PreparedStatement, idx: Int): Unit =
        p.setTimestamp(idx, primitiveTimestampToSqlTimestamp(v), utcCalendar)
      def getValue(r: ResultSet, idx: Int): P.Timestamp =
        sqlTimestampToPrimitiveTimestamp(r.getTimestamp(idx, utcCalendar))
      def updateValue(v: P.Timestamp, r: ResultSet, idx: Int): Unit =
        r.updateTimestamp(idx, primitiveTimestampToSqlTimestamp(v))
      override def valueToSQLLiteral(value: P.Timestamp): String =
        profile.columnTypes.timestampJdbcType
          .valueToSQLLiteral(primitiveTimestampToSqlTimestamp(value))
    }

  def primitiveListColumnType[A: F]: BaseColumnType[P.List[A]] = {
    val ev1 = implicitly[F[A]]
    val ev2 = implicitly[ClassTag[P.List[A]]]
    val ev3 = implicitly[ColumnType[T]]
    profile.MappedColumnType
      .base[P.List[A], T](a => listCodec.encodeList(a)(ev1), b => listCodec.decodeList(b)(ev1))(
        ev2,
        ev3)
  }

  def primitiveOptionalColumnType[A: F]: BaseColumnType[P.Optional[A]] = {
    val ev1 = implicitly[F[A]]
    val ev2 = implicitly[ClassTag[P.Optional[A]]]
    val ev3 = implicitly[ColumnType[T]]
    profile.MappedColumnType
      .base[P.Optional[A], T](
        a => listCodec.encodeOption(a)(ev1),
        b => listCodec.decodeOption(b)(ev1))(ev2, ev3)
  }

  def primitiveMapColumnType[A: F]: BaseColumnType[P.Map[A]] = {
    val ev1 = implicitly[F[A]]
    val ev2 = implicitly[ClassTag[P.Map[A]]]
    val ev3 = implicitly[ColumnType[T]]
    profile.MappedColumnType
      .base[P.Map[A], T](a => listCodec.encodeMap(a)(ev1), b => listCodec.decodeMap(b)(ev1))(
        ev2,
        ev3)
  }
}

object CustomJdbcTypes {
  def apply[T, F[_], Profile <: SupportedProfile with Singleton](
      profile: Profile,
      listCodec: ListOptionMapCodec[T, F])(
      implicit T: profile.ColumnType[T]): CustomJdbcTypes[T, F, profile.type] =
    new CustomJdbcTypes[T, F, profile.type](profile, listCodec)
}
