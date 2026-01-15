// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import com.digitalasset.transcode.schema

/** This trait describes various types that a Daml package can contain. `SchemaProcessor` will use
  * an implementation of this trait to feed types into the visitor. The visitor implementation
  * should define [[SchemaVisitor.Type]], which can be a Codec, or a Code-generator along with
  * instructions how to process daml types. There are several use cases:
  *
  * ==Codecs==
  *
  * To achieve the best performance, a codec should create a tree-like structure copying the
  * structure of Daml types with each node processing corresponding daml type and delegating
  * processing to the next node if it's a type container.
  *
  * A codec should convert to and from [[DynamicValue]] instances. This allows to compose codecs
  * from various protocols by combining them in [[com.digitalasset.transcode.Converter]]. For
  * example, one code combine `JsonCodec` and `ProtobufCodec` to get direct conversions from json to
  * Ledger API proto values and vice versa. Or one can compose `JsonCodec` and `ScalaCodec`, etc.
  *
  * Note, that Dynamic Values omit some type information (like field names or variant names) to
  * reduce memory allocation and serialization. This information is known to the codec at the type
  * of construction and codecs are encouraged to cache it.
  *
  * ==Code generators==
  *
  * Code generators can produce code snippets at each handler and combine them into a file or a set
  * of files that can be used as generated source in the target language.
  *
  * It is advisable to also generate a codec along with DTOs (Data Transfer Object) to allow for
  * direct interoperability with other existing protocols (Json or Protobuf).
  */
trait SchemaVisitor:
  /** Visitor handler type for various DAML schema cases. */
  type Type

  /** Final result type */
  type Result

  ///////////////////////////
  // Top-level definitions //
  ///////////////////////////

  def collect(entities: Seq[Template[Type]]): Result

  //////////////////////////
  // Algebraic data types //
  //////////////////////////

  // NB: Be aware of potential recursive definitions. Don't instantiate call-by-name parameters in the constructor.
  // Alternatively, protect against the loops

  /** ADT, Product type */
  def record(fields: Seq[(FieldName, Type)]): Type

  /** ADT, Sum type */
  def variant(cases: Seq[(VariantConName, Type)]): Type

  /** ADT, Sum type - special case, where there are only named constructors without arguments */
  def enumeration(cases: Seq[EnumConName]): Type

  ///////////////////////////
  // built-in traversables //
  ///////////////////////////

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

  /** Contract ID, parametrized with the processor for corresponding template */
  def contractId(template: Type): Type

  ///////////
  // other //
  ///////////

  /** Type Variable.
    *
    * Codecs will use substitution, effectively replacing type variables with concrete types, while
    * code generators will use type variable names.
    */
  def variable(name: TypeVarName): Type

  /** Wrap type into addressable DataType */
  def constructor(id: Identifier, typeParams: Seq[TypeVarName], value: => Type): Type

  /** Type Application */
  def application(value: Type, typeParams: Seq[TypeVarName], args: Seq[Type]): Type
end SchemaVisitor

object SchemaVisitor:

  def compose[T](left: SchemaVisitor, right: SchemaVisitor) = new SchemaVisitor:
    override type Result = (left.Result, right.Result)
    override type Type = (left.Type, right.Type)

    override def collect(
        entities: Seq[Template[(left.Type, right.Type)]]
    ): (left.Result, right.Result) =
      val (leftEntities, rightEntities) = entities
        .map(t =>
          (
            Template(
              t.templateId,
              t.payload._1,
              t.key.map(_._1),
              t.isInterface,
              t.implements,
              t.choices.map(c =>
                Choice(
                  c.name,
                  c.consuming,
                  c.argument._1,
                  c.result._1,
                )
              ),
            ),
            Template(
              t.templateId,
              t.payload._2,
              t.key.map(_._2),
              t.isInterface,
              t.implements,
              t.choices.map(c =>
                Choice(
                  c.name,
                  c.consuming,
                  c.argument._2,
                  c.result._2,
                )
              ),
            ),
          )
        )
        .unzip
      (left.collect(leftEntities), right.collect(rightEntities))

    override def record(fields: Seq[(FieldName, Type)]): Type =
      val (leftFields, rightFields) = fields.map { case (n, (l, r)) => ((n, l), (n, r)) }.unzip
      (left.record(leftFields), right.record(rightFields))
    override def variant(cases: Seq[(VariantConName, Type)]): Type =
      val (leftCases, rightCases) = cases.map { case (n, (l, r)) => ((n, l), (n, r)) }.unzip
      (left.variant(leftCases), right.variant(rightCases))
    override def enumeration(cases: Seq[EnumConName]): Type =
      (left.enumeration(cases), right.enumeration(cases))
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
    override def variable(name: TypeVarName): Type = (left.variable(name), right.variable(name))
    override def constructor(id: Identifier, typeParams: Seq[TypeVarName], body: => Type): Type =
      lazy val (leftBody, rightBody) = body
      (left.constructor(id, typeParams, leftBody), right.constructor(id, typeParams, rightBody))
    def application(
        ctor: (left.Type, right.Type),
        typeParams: Seq[TypeVarName],
        args: Seq[(left.Type, right.Type)],
    ): (left.Type, right.Type) =
      val (leftCtor, rightCtor) = ctor
      val (leftArgs, rightArgs) = args.unzip
      (
        left.application(leftCtor, typeParams, leftArgs),
        right.application(rightCtor, typeParams, rightArgs),
      )

  /** Trait implementing all the cases and allowing to override only partially what's needed */
  trait Unit extends SchemaVisitor:
    type Type = _root_.scala.Unit
    override def record(fields: Seq[(FieldName, Type)]): Type = {}
    override def variant(cases: Seq[(VariantConName, Type)]): Type = {}
    override def enumeration(cases: Seq[EnumConName]): Type = {}
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
    override def constructor(
        id: Identifier,
        typeParams: Seq[TypeVarName],
        value: => Type,
    ): Type = {}
    override def variable(name: TypeVarName): Type = {}
    override def application(value: Type, typeParams: Seq[TypeVarName], args: Seq[Type]): Type = {}
  end Unit

  trait Delegate[T <: SchemaVisitor, R](protected val delegate: T)(
      protected val conv: delegate.Result => R
  ) extends SchemaVisitor
      with WithResult[R]:

    type Type = delegate.Type

    final def collect(entities: Seq[Template[delegate.Type]]): R = conv(delegate.collect(entities))

    def record(fields: Seq[(FieldName, Type)]): Type = delegate.record(fields)
    def variant(cases: Seq[(VariantConName, Type)]): Type = delegate.variant(cases)
    def enumeration(cases: Seq[EnumConName]): Type = delegate.enumeration(cases)
    def list(elem: Type): Type = delegate.list(elem)
    def optional(elem: Type): Type = delegate.optional(elem)
    def textMap(value: Type): Type = delegate.textMap(value)
    def genMap(key: Type, value: Type): Type = delegate.genMap(key, value)
    def unit: Type = delegate.unit
    def bool: Type = delegate.bool
    def text: Type = delegate.text
    def int64: Type = delegate.int64
    def numeric(scale: Int): Type = delegate.numeric(scale)
    def timestamp: Type = delegate.timestamp
    def date: Type = delegate.date
    def party: Type = delegate.party
    def contractId(template: Type): Type = delegate.contractId(template)
    def variable(name: TypeVarName): Type = delegate.variable(name)
    def constructor(id: Identifier, typeParams: Seq[TypeVarName], value: => Type): Type =
      delegate.constructor(id, typeParams, value)
    def application(value: Type, typeParams: Seq[TypeVarName], args: Seq[Type]): Type =
      delegate.application(value, typeParams, args)

  trait WithResult[R] extends SchemaVisitor:
    final type Result = R
end SchemaVisitor
