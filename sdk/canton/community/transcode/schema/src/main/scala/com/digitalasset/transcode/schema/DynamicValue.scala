// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

/** Dynamic types are represented by opaque types to reduce memory allocations during transformation
  * from one codec to another.
  *
  * There are 3 kinds of possible dynamic values:
  *   - ADTs: records or variants. These are fixed in size, and are containers to other types. These
  *     types are addressable and have identifiers. There are special cases: enums and wrapped
  *     types.
  *   - Traversables: lists, maps, optionals. These are variable in size and are containers to other
  *     types.
  *   - Primitives: primitive scalar types.
  *
  * Codecs and code-generations should be constructed in such a way that ADTs and Traversables
  * expecting other underlying dynamic values in processing routines should know exactly what
  * underlying values they expect. The unwrapping of dynamic value and casting should be safe then.
  * SchemaProcessor takes care of constructing a tree of type processors and injects correct
  * underlying processors where needed. See examples of how this is used in codecs: JSON and Grpc
  * and code-gens: Java and Scala.
  */
object DynamicValue:
  opaque type Type = Any

  ///////////
  // Kinds //
  ///////////

  opaque type Adt <: DynamicValue = Any
  opaque type Traversable <: DynamicValue = Any
  opaque type Primitive <: DynamicValue = Any

  //////////////////////////
  // Algebraic data types //
  //////////////////////////

  /** ADT, Product type. Field values have to be in the same order os in the defining type. Field
    * names here are not necessary, as codecs knows about field names at the time of construction
    */
  opaque type Record <: Adt = Array[DynamicValue]
  def Record(fields: Array[DynamicValue]): Record = fields
  def Record(fields: IterableOnce[DynamicValue]): Record = Array.from(fields)
  def Record(fields: DynamicValue*): Record = Array.from(fields)

  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    private def record: Record = value.asInstanceOf[Record]
    def checkRecordSize(maxSize: Int): scala.Unit =
      val rec = value.record
      if maxSize < rec.length
      then require(rec.iterator.drop(maxSize).forall(_ == None), "Unexpected field in record")
    def recordIterator: Iterator[DynamicValue] = value.record.iterator
    def recordIteratorPadded(expectedFieldCount: Int): Iterator[DynamicValue] =
      value.record.iterator
        .concat(Iterator.continually(DynamicValue.Optional(None)))
        .take(expectedFieldCount)
  def withRecordIterator[A](
      dv: DynamicValue,
      expectedFieldCount: Int,
      code: java.util.function.Function[Iterator[DynamicValue], A],
  ): A =
    dv.checkRecordSize(expectedFieldCount)
    code(dv.recordIteratorPadded(expectedFieldCount))

  /** ADT, Sum type. Contains constructor's ordinal index and a wrapped value. */
  opaque type Variant <: Adt = (Int, DynamicValue)
  def Variant(ctorIx: Int, value: DynamicValue): Variant = (ctorIx, value)
  extension [B](value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def variant: (Int, DynamicValue) = value.asInstanceOf[Variant]
    def variantIx: Int = value.variant._1
    def variantValue: DynamicValue = value.variant._2

  /** ADT, Sum type - special case of sum type with parameter-less constructors. */
  opaque type Enumeration <: Adt = Int
  def Enumeration(ctorIx: Int): Enumeration = ctorIx
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def enumeration: Int = value.asInstanceOf[Enumeration]

  ///////////////////////
  // Traversable types //
  ///////////////////////

  /** Sequence of elements */
  opaque type List <: Traversable = Array[DynamicValue]
  def List(value: IterableOnce[DynamicValue]): List = Array.from(value)
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def list: IterableOnce[DynamicValue] = value.asInstanceOf[List].iterator

  /** Optional element */
  opaque type Optional <: Traversable = Option[DynamicValue]
  def Optional(value: Option[DynamicValue]): Optional = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def optional: Option[DynamicValue] = value.asInstanceOf[Optional]
    def isEmpty: Boolean = value match
      case None => true
      case _ => false

  /** Map with String keys. Codecs should maintain stable order of key-value entries if possible. */
  opaque type TextMap <: Traversable = Array[(String, DynamicValue)]
  def TextMap(value: IterableOnce[(String, DynamicValue)]): TextMap = Array.from(value)
  extension (v: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def textMap: IterableOnce[(String, DynamicValue)] = v.asInstanceOf[TextMap].iterator

  /** Map with arbitrarily typed keys and values. Codecs should maintain stable order of key-value
    * entries if possible.
    */
  opaque type GenMap <: Traversable = Array[(DynamicValue, DynamicValue)]
  def GenMap(value: IterableOnce[(DynamicValue, DynamicValue)]): GenMap = Array.from(value)
  extension (v: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def genMap: IterableOnce[(DynamicValue, DynamicValue)] = v.asInstanceOf[GenMap].iterator

  /////////////////////
  // Primitive Types //
  /////////////////////

  /** Unit */
  opaque type Unit <: Primitive = scala.Unit
  val Unit: Unit = ()
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def unit: scala.Unit = value.asInstanceOf[Unit]

  /** Boolean */
  opaque type Bool <: Primitive = Boolean
  def Bool(value: Boolean): Bool = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def bool: Boolean = value.asInstanceOf[Bool]

  /** Text */
  opaque type Text <: Primitive = String
  def Text(value: String): Text = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def text: String = value.asInstanceOf[Text]

  /** 8-byte integer */
  opaque type Int64 <: Primitive = Long
  def Int64(value: Long): Int64 = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def int64: Long = value.asInstanceOf[Int64]

  /** Numeric type with precision. Represented as a String */
  opaque type Numeric <: Primitive = String
  def Numeric(value: String): Numeric = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def numeric: String = value.asInstanceOf[Numeric]

  /** Timestamp. Number of microseconds (10^-6^) since epoch (midnight of 1 Jan 1970) in UTC
    * timezone.
    */
  opaque type Timestamp <: Primitive = Long
  def Timestamp(value: Long): Timestamp = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def timestamp: Long = value.asInstanceOf[Timestamp]

  /** Local date. Number of dates since epoch (1 Jan 1970). */
  opaque type Date <: Primitive = Int
  def Date(value: Int): Date = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def date: Int = value.asInstanceOf[Date]

  /** Party */
  opaque type Party <: Primitive = String
  def Party(value: String): Party = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def party: String = value.asInstanceOf[Party]

  /** Contract ID */
  opaque type ContractId <: Primitive = String
  def ContractId(value: String): ContractId = value
  extension (value: DynamicValue)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def contractId: String = value.asInstanceOf[ContractId]

  def equals(a: DynamicValue, b: DynamicValue): Boolean = (a, b) match
    case (aa: Array[?], bb: Array[?]) if aa.length == bb.length =>
      (aa.iterator zip bb.iterator).forall(equals(_, _))
    case (aa: Array[?], bb: Array[?]) =>
      val prefixEqual = (aa.iterator zip bb.iterator).forall(equals(_, _))
      val prefixSize = aa.length min bb.length
      val suffixIsNone =
        aa.drop(prefixSize).forall(_ == None) && bb.drop(prefixSize).forall(_ == None)
      prefixEqual && suffixIsNone
    case (Some(aa), Some(bb)) => equals(aa, bb)
    case ((a1, a2), (b1, b2)) => equals(a1, b1) && equals(a2, b2)
    case (a, b) => a == b

end DynamicValue
