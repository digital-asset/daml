// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

/** This trait describes various types that a Daml package can contain. `SchemaProcessor` will use an implementation of
  * this trait to feed types into the visitor. The visitor implementation should define [[SchemaVisitor.Type]], which
  * can be a Codec, or a Code-generator along with instructions how to process daml types. There are several use cases:
  *
  * ==Codecs==
  *
  * To achieve the best performance, a codec should create a tree-like structure copying the structure of Daml types
  * with each node processing corresponding daml type and delegating processing to the next node if it's a type
  * container.
  *
  * A codec should convert to and from [[DynamicValue]] instances. This allows to compose codecs from various protocols
  * by combining them in [[com.digitalasset.transcode.Converter]]. For example, one code combine `JsonCodec` and
  * `GrpcValueCodec` to get direct conversions from json to Ledger API proto values and vice versa. Or one can compose
  * `JsonCodec` and `ScalaCodec`, etc.
  *
  * ==Code generators==
  *
  * Code generators can produce code snippets at each handler and combine them into a file or a set of files that can be
  * used as generated source in the target language.
  *
  * It is advisable to also generate a codec along with DTOs (Data Transfer Object) to allow for direct interoperability
  * with other existing protocols (Json or Protobuf).
  */
trait SchemaVisitor {

  /** Visitor handler type for various DAML schema cases.
    */
  type Type

  //////////////////////////
  // Algebraic data types //
  //////////////////////////

  // NB: Be aware of potential recursive definitions. Don't instantiate call-by-name parameters in the constructor.
  // Alternatively, protect against the loops

  /** ADT, Product type */
  def record(
      id: Identifier,
      appliedArgs: => Seq[(TypeVarName, Type)],
      fields: => Seq[(FieldName, Type)],
  ): Type

  /** ADT, Sum type */
  def variant(
      id: Identifier,
      appliedArgs: => Seq[(TypeVarName, Type)],
      cases: => Seq[(VariantConName, Type)],
  ): Type

  /** ADT, Sum type - special case, where there are only named constructors without further payloads */
  def `enum`(id: Identifier, cases: Seq[EnumConName]): Type

  //////////////////
  // traversables //
  //////////////////

  /** Sequence of elements */
  def list(elem: Type): Type

  /** Optional element */
  def optional(elem: Type): Type

  /** Map with keys of String/Text type */
  def textMap(value: Type): Type

  /** Map with keys and values of any type */
  def genMap(key: Type, value: Type): Type

  ////////////////
  // primitives //
  ////////////////

  /** Unit */
  def unit: Type

  /** Boolean */
  def bool: Type

  /** Text */
  def text: Type

  /** 8-byte Integer */
  def int64: Type

  /** Numeric with scale */
  def numeric(scale: Int): Type

  /** Timestamp */
  def timestamp: Type

  /** Date */
  def date: Type

  /** Party */
  def party: Type

  /** Contract Id, parametrized with the processor for corresponding template */
  def contractId(template: Type): Type

  ///////////
  // other //
  ///////////

  /** Interface. Used in code-gens. There is no representation of interface in Dynamic Value */
  def interface(name: Identifier): Type

  /** Type Variable.
    *
    * Codecs might want to use `value` substitution, effectively replacing type variables with concrete types, while
    * code generators might want to use type variable names.
    */
  def variable(name: TypeVarName, value: Type): Type

}

object SchemaVisitor {

  def compose[T](left: SchemaVisitor, right: SchemaVisitor) = new SchemaVisitor {

    type Type = (left.Type, right.Type)

    private def lefts[A](seq: Seq[(A, Type)]): Seq[(A, left.Type)] = seq.map { case (n, t) =>
      (n, t._1)
    }

    private def rights[A](seq: Seq[(A, Type)]): Seq[(A, right.Type)] = seq.map { case (n, t) =>
      (n, t._2)
    }

    override def record(
        id: Identifier,
        appliedArgs: => Seq[(TypeVarName, Type)],
        fields: => Seq[(FieldName, Type)],
    ): Type =
      (
        left.record(id, lefts(appliedArgs), lefts(fields)),
        right.record(id, rights(appliedArgs), rights(fields)),
      )

    override def variant(
        id: Identifier,
        appliedArgs: => Seq[(TypeVarName, Type)],
        cases: => Seq[(VariantConName, Type)],
    ): Type =
      (
        left.variant(id, lefts(appliedArgs), lefts(cases)),
        right.variant(id, rights(appliedArgs), rights(cases)),
      )

    override def `enum`(id: Identifier, cases: Seq[EnumConName]): Type =
      (left.`enum`(id, cases), right.`enum`(id, cases))

    override def list(elem: Type): Type = (left.list(elem._1), right.list(elem._2))

    override def optional(elem: Type): Type = (left.optional(elem._1), right.optional(elem._2))

    override def textMap(value: Type): Type = (left.textMap(value._1), right.textMap(value._2))

    override def genMap(key: Type, value: Type): Type =
      (left.genMap(key._1, value._1), right.genMap(key._2, value._2))

    override def unit: Type = (left.unit, right.unit)

    override def bool: Type = (left.bool, right.bool)

    override def text: Type = (left.text, right.text)

    override def int64: Type = (left.int64, right.int64)

    override def numeric(scale: Int): Type = (left.numeric(scale), right.numeric(scale))

    override def timestamp: Type = (left.timestamp, right.timestamp)

    override def date: Type = (left.date, right.date)

    override def party: Type = (left.party, right.party)

    override def contractId(template: Type): Type =
      (left.contractId(template._1), right.contractId(template._2))

    override def interface(id: Identifier): Type = (left.interface(id), right.interface(id))

    override def variable(name: TypeVarName, value: Type): Type =
      (left.variable(name, value._1), right.variable(name, value._2))

  }

  /** Trait implementing all the cases and allowing to override only partially what's needed */
  trait Unit extends SchemaVisitor {
    type Type = _root_.scala.Unit

    override def record(
        id: Identifier,
        appliedArgs: => Seq[(TypeVarName, Type)],
        fields: => Seq[(FieldName, Type)],
    ): Type = {}

    override def variant(
        id: Identifier,
        appliedArgs: => Seq[(TypeVarName, Type)],
        cases: => Seq[(VariantConName, Type)],
    ): Type = {}

    override def `enum`(id: Identifier, cases: Seq[EnumConName]): Type = {}

    override def list(elem: Type): Type = {}

    override def optional(elem: Type): Type = {}

    override def textMap(value: Type): Type = {}

    override def genMap(key: Type, value: Type): Type = {}

    override def unit: Type = {}

    override def bool: Type = {}

    override def text: Type = {}

    override def int64: Type = {}

    override def numeric(scale: Int): Type = {}

    override def timestamp: Type = {}

    override def date: Type = {}

    override def party: Type = {}

    override def contractId(template: Type): Type = {}

    override def interface(id: Identifier): Type = {}

    override def variable(name: TypeVarName, value: Type): Type = {}
  }
}
