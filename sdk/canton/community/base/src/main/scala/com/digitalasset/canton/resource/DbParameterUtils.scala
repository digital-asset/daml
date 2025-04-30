// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import slick.jdbc.{GetResult, PositionedParameters}

import java.sql.{JDBCType, SQLNonTransientException}
import scala.reflect.ClassTag

/** This trait provides utility methods for database operations, specifically for retrieving integer
  * array data from parameters in the database.
  */
object DbParameterUtils {

  /** Sets an array of integers as a database parameter.
    *
    * @param maybeArray
    *   An optional array of integers. If `None`, the parameter is set to null.
    * @param pp
    *   A `PositionedParameters` object, which is used to set the database parameter.
    */
  def setArrayIntOParameterDb(
      maybeArray: Option[Array[Int]],
      pp: PositionedParameters,
  ): Unit = {
    val jdbcArray = maybeArray
      .map(_.map(id => Int.box(id): AnyRef))
      .map(pp.ps.getConnection.createArrayOf("integer", _))

    pp.setObjectOption(jdbcArray, JDBCType.ARRAY.getVendorTypeNumber)
  }

  /** Retrieves an array of integers from the database and deserializes it to the correct type. This
    * function supports different database profiles (H2 and Postgres) for handling array results.
    *
    * @param storageProfile
    *   lists the type of storage (i.e. H2 or Postgres).
    * @param deserialize
    *   A function that converts an `Int` into type `A`.
    * @tparam A
    *   The type to which the integers will be deserialized.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  def getDataArrayOResultsDb[A: ClassTag](
      storageProfile: DbStorage.Profile,
      deserialize: Int => A,
  ): GetResult[Option[Array[A]]] = {

    def toInt(a: AnyRef) =
      storageProfile match {
        case _: H2 =>
          a match {
            case s: String => s.toInt
            case null =>
              throw new SQLNonTransientException(s"Cannot convert object array element null to Int")
            case invalid =>
              throw new SQLNonTransientException(
                s"Cannot convert object array element (of type ${invalid.getClass.getName}) to Int"
              )
          }
        case _: Postgres => Int.unbox(a)
      }

    GetResult(r => Option(r.rs.getArray(r.skip.currentPos)))
      .andThen(_.map(_.getArray.asInstanceOf[Array[AnyRef]].map(toInt)))
      .andThen(_.map(_.map(data => deserialize(data))))
  }

  /** Sets an array of A as a database parameter, serializing each item into bytes.
    *
    * @param storageProfile
    *   lists the type of storage (i.e. H2 or Postgres).
    * @param serialize
    *   A function that converts an `A` into `Array[Byte]`.
    * @param arrayBytes
    *   An array of array of bytes.
    * @param pp
    *   A `PositionedParameters` object, which is used to set the database parameter.
    * @tparam A
    *   The type to which each array of bytes will be deserialized.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def setArrayBytesParameterDb[A](
      storageProfile: DbStorage.Profile,
      items: Array[A],
      serialize: A => Array[Byte],
      pp: PositionedParameters,
  ): Unit = {
    val blobTypeName = storageProfile match {
      case _: H2 => "binary large object"
      case _: Postgres => "bytea"
    }
    val jdbcArray = pp.ps.getConnection.createArrayOf(
      blobTypeName,
      items.map(serialize).asInstanceOf[Array[AnyRef]],
    )
    pp.setObject(jdbcArray, JDBCType.ARRAY.getVendorTypeNumber)
  }

  /** Retrieves an array of array of bytes from the database and deserializes it to an array of A.
    *
    * @param deserialize
    *   A function that converts an `Array[Byte]` into type `A`.
    * @tparam A
    *   The type to which each array of bytes will be deserialized.
    */
  def getDataBytesArrayResultsDb[A: ClassTag](
      deserialize: Array[Byte] => A
  ): GetResult[Array[A]] = {

    def toBlobBytes(obj: AnyRef): Array[Byte] = obj match {
      case blob: java.sql.Blob => blob.getBytes(1, blob.length.toInt)
      case invalid =>
        throw new SQLNonTransientException(
          s"Cannot convert object array element (of type ${invalid.getClass.getName}) to byte array"
        )
    }

    GetResult(r => r.rs.getArray(r.skip.currentPos))
      .andThen { case (sqlArr: java.sql.Array) =>
        sqlArr.getArray match {
          case arr: Array[Array[Byte]] => arr // Postgres
          case arr: Array[AnyRef] => arr.map(toBlobBytes(_)) // H2
          case other =>
            throw new SQLNonTransientException(
              s"Cannot convert object (of type ${other.getClass.getName}) to byte array array"
            )
        }
      }
      .andThen(_.map(deserialize))
  }

}
