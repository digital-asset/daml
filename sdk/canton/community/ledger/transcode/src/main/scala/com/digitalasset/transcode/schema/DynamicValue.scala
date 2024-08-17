// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import scala.annotation.nowarn

/** There are 3 kinds of possible dynamic values:
  *   - ADTs: records or variants. These are fixed in size, and are containers to other types.
  *   - Traversables: lists, maps, optionals. These are variable in size and are containers to other types.
  *   - Primitives: primitive scalar types.
  *
  * Codecs and code-generations should be constructed in such a way that ADTs and Traversables expecting other
  * underlying dynamic values in processing routines should know exactly what underlying values they expect. The
  * unwrapping of dynamic value and casting should be safe then. SchemaProcessor takes care of constructing a tree of
  * type processors and injects correct underlying processors where needed. Also see examples of how this is used in
  * JSON and Grpc codecs.
  */

sealed trait DynamicValue {
  def inner: Any
}

//suppression reason: code translated from scala3 (opaque type)
@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object DynamicValue {

  ///////////
  // Kinds //
  ///////////
  trait Adt extends DynamicValue
  trait Traversable extends DynamicValue
  trait Primitive extends DynamicValue

  //////////////////////////
  // Algebraic data types //
  //////////////////////////

  /** ADT, Product type. Field values have to be in the same order os in the defining type. Field names here are not
    * necessary, as codecs knows about field names at the time of construction
    */

  final case class Record(fields: IterableOnce[DynamicValue]) extends Adt {
    override def inner: Any = fields
  }

  implicit class RecordExtension(value: DynamicValue) {
    def record: IterableOnce[DynamicValue] = value.inner.asInstanceOf[IterableOnce[DynamicValue]]
  }

  /** ADT, Sum type. Contains constructor's ordinal index and a wrapped value. */

  final case class Variant(ctorIx: Int, value: DynamicValue) extends Adt {
    override def inner: Any = value
  }

  implicit class VariantExtension(value: DynamicValue) {
    def variant: Variant = value.asInstanceOf[Variant]
  }

  /** ADT, Sum type - special case of sum type with only constructors. */

  final case class Enum(value: Int) extends Adt {
    override def inner: Any = value
  }

  implicit class EnumExtension(value: DynamicValue) {
    def `enum`: Int = value.asInstanceOf[Enum].value
  }

  ///////////////////////
  // Traversable types //
  ///////////////////////

  /** Sequence of elements */
  final case class List(value: IterableOnce[DynamicValue]) extends Traversable {
    override def inner: Any = value
  }
  implicit class ListExtension(value: DynamicValue) {
    def list: IterableOnce[DynamicValue] = value.inner.asInstanceOf[IterableOnce[DynamicValue]]
  }

  /** Optional element */
  final case class Optional(value: Option[DynamicValue]) extends Traversable {
    override def inner: Any = value
  }

  implicit class OptionalExtension(value: DynamicValue) {
    def optional: Option[DynamicValue] = value.inner.asInstanceOf[Option[DynamicValue]]
  }

  /** Map with String keys. Codecs should maintain stable order of key-value entries if possible. */
  final case class TextMap(value: IterableOnce[(String, DynamicValue)]) extends Traversable {
    override def inner: Any = value
  }
  implicit class TextMapExtension(value: DynamicValue) {
    def textMap: IterableOnce[(String, DynamicValue)] =
      value.asInstanceOf[IterableOnce[(String, DynamicValue)]]
  }

  /** Map with arbitrarily-typed keys and values. Codecs should maintain stable order of key-value entries if possible.
    */
  final case class GenMap(value: IterableOnce[(DynamicValue, DynamicValue)]) extends Traversable {
    override def inner: Any = value
  }

  implicit class GenMapExtension(value: DynamicValue) {
    def genMap: IterableOnce[(DynamicValue, DynamicValue)] =
      value.inner.asInstanceOf[IterableOnce[(DynamicValue, DynamicValue)]]
  }

  /////////////////////
  // Primitive Types //
  /////////////////////

  /** Unit */
  case object Unit extends Primitive {
    override def inner: Any = Unit
  }
  implicit class UnitExtension(value: DynamicValue) {
    @nowarn def unit: scala.Unit = value.inner.asInstanceOf[Unit]
  }

  /** Boolean */
  final case class Bool(value: Boolean) extends Primitive {
    override def inner: Any = value
  }
  implicit class BoolExtension(value: DynamicValue) {
    def bool: Boolean = value.inner.asInstanceOf[Boolean]
  }

  /** Text */
  final case class Text(value: String) extends Primitive {
    override def inner: Any = value
  }
  implicit class TextExtension(value: DynamicValue) {
    def text: String = value.inner.asInstanceOf[String]
  }

  /** 8-byte integer */
  final case class Int64(value: Long) extends Primitive {
    override def inner: Any = value
  }

  implicit class Int64Extension(value: DynamicValue) {
    def int64: Long = value.inner.asInstanceOf[Long]
  }

  /** Numeric type with precision. Represented as a String */
  final case class Numeric(value: String) extends Primitive {
    override def inner: Any = value
  }

  implicit class NumericExtension(value: DynamicValue) {
    def numeric: String = value.inner.asInstanceOf[String]
  }

  /** Timestamp. Number of microseconds (10^-6^) since epoch (midnight of 1 Jan 1970) in UTC timezone. */
  final case class Timestamp(value: Long) extends Primitive {
    override def inner: Any = value
  }

  implicit class TimestampExtension(value: DynamicValue) {
    def timestamp: Long = value.inner.asInstanceOf[Long]
  }

  /** Local date. Number of dates since epoch (1 Jan 1970). */
  final case class Date(value: Int) extends Primitive {
    override def inner: Any = Int
  }

  implicit class DateExtension(value: DynamicValue) {
    def date: Int = value.inner.asInstanceOf[Int]
  }

  final case class Party(value: String) extends Primitive {
    override def inner: Any = value
  }

  implicit class PartyExtension(value: DynamicValue) {
    def party: String = value.inner.asInstanceOf[String]
  }

  final case class ContractId(value: String) extends Primitive {
    override def inner: Any = value
  }

  implicit class ContractIdExtension(value: DynamicValue) {
    def contractId: String = value.inner.asInstanceOf[String]
  }
}
