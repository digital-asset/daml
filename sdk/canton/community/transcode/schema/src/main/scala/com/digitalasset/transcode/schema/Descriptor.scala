// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import com.digitalasset.transcode.schema.*

import scala.PartialFunction.condOpt
import scala.annotation.varargs
import scala.collection.immutable.List as SList
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.util.hashing.MurmurHash3

/** Compile-time representation of Schema. There are 4 main types of descriptors:
  *
  *   - '''Algebraic Data Types''': Record, Variant, Enumeration.
  *   - '''Traversables''': List, Optional, TextMap, GenMap
  *   - '''Primitive Types''': Unit, Bool, Text, Int64, Numeric, Timestamp, Date, Party, ContractId
  *   - '''Type Constructors''' and Generic Types: Constructor, Application, Variable
  */
sealed trait Descriptor(using sourcecode.Name) extends HasDescriptor {
  final def getDescriptor: Descriptor = this
  final val descriptorName: String = summon[sourcecode.Name].value
}
object Descriptor:

  ///////////
  // Kinds //
  ///////////

  sealed trait Adt extends Descriptor
  sealed trait Traversable extends Descriptor
  sealed trait Primitive extends Descriptor

  /** Product data type. Has a list of named fields. */
  final case class Record private[Descriptor] (fields: SList[(FieldName, Descriptor)]) extends Adt
  def record(): Record =
    record(Seq.empty)
  @varargs def record(field: (String, Descriptor), fields: (String, Descriptor)*): Record =
    record(field +: fields)
  def record(fields: Seq[(String, Descriptor)]): Record =
    Record(fields.map((name, descriptor) => FieldName(name) -> descriptor).toList)
  object Record:
    object Ctor:
      def unapply(
          d: Descriptor
      ): Option[(Identifier, SList[TypeVarName], SList[(FieldName, Descriptor)])] = for
        (id, typeParams, body) <- condOpt(d) {
          case Constructor(id, tp, body) if !id.entityName.contains('.') => (id, tp, body)
        }
        fields <- condOpt(body) { case Record(fields) => fields }
      yield (id, typeParams, fields)
    object Embedded:
      def unapply(d: Descriptor): Option[(Identifier, SList[(FieldName, Descriptor)])] =
        condOpt(d) {
          case Constructor.Applied(id, _, Record(fields)) if id.entityName.contains('.') =>
            (id, fields)
        }

  /** Sum data type. Exclusive union of named data structures or values */
  final case class Variant private[Descriptor] (cases: SList[(VariantConName, Descriptor)])
      extends Adt
  @varargs def variant(`case`: (String, Descriptor), cases: (String, Descriptor)*): Variant =
    variant(`case` +: cases)
  def variant(cases: Seq[(String, Descriptor)]): Variant =
    Variant(cases.map((name, descriptor) => VariantConName(name) -> descriptor).toList)
  object Variant:
    object Ctor:
      def unapply(
          d: Descriptor
      ): Option[(Identifier, SList[TypeVarName], SList[(VariantConName, Descriptor)])] = for
        (id, typeParams, body) <- condOpt(d) { case Constructor(id, tp, body) => (id, tp, body) }
        cases <- condOpt(body) { case Variant(cases) => cases }
      yield (id, typeParams, cases)

  /** Sum data type. Exclusive union of symbols. */
  final case class Enumeration private[Descriptor] (cases: SList[EnumConName]) extends Adt
  @varargs def enumeration(`case`: String, cases: String*): Enumeration = enumeration(
    `case` +: cases
  )
  def enumeration(cases: Seq[String]): Enumeration = Enumeration(cases.map(EnumConName).toList)
  object Enumeration:
    object Ctor:
      def unapply(d: Descriptor): Option[(Identifier, SList[EnumConName])] = for
        (id, body) <- condOpt(d) { case Constructor(id, tp, body) => (id, body) }
        cases <- condOpt(body) { case Enumeration(cases) => cases }
      yield (id, cases)

  /** List of values of the same type */
  final case class List private[Descriptor] (value: Descriptor) extends Traversable
  def list(value: Descriptor): List = List(value)

  /** Optional value */
  final case class Optional private[Descriptor] (value: Descriptor) extends Traversable
  def optional(value: Descriptor): Optional = Optional(value)

  /** Map with string keys */
  final case class TextMap private[Descriptor] (value: Descriptor) extends Traversable
  def textMap(value: Descriptor): TextMap = TextMap(value)

  /** Map */
  final case class GenMap private[Descriptor] (key: Descriptor, value: Descriptor)
      extends Traversable
  def genMap(key: Descriptor, value: Descriptor): GenMap = GenMap(key, value)

  /** Singleton type */
  case object Unit extends Primitive
  val unit: Unit.type = Unit

  /** Boolean */
  case object Bool extends Primitive
  val bool: Bool.type = Bool

  /** String */
  case object Text extends Primitive
  val text: Text.type = Text

  /** 64-bit integer */
  case object Int64 extends Primitive
  val int64: Int64.type = Int64

  /** Rational Number with a predefined scale */
  final case class Numeric private[Descriptor] (scale: Int) extends Primitive
  def numeric(scale: Int): Numeric = Numeric(scale)

  /** Timestamp */
  case object Timestamp extends Primitive
  val timestamp: Timestamp.type = Timestamp

  /** Date */
  case object Date extends Primitive
  val date: Date.type = Date

  /** Party ID */
  case object Party extends Primitive
  val party: Party.type = Party

  /** Contract ID */
  final case class ContractId private[Descriptor] (value: Descriptor) extends Primitive
  def contractId(value: Descriptor): ContractId = ContractId(value)

  /** Type Constructor with a fully qualified name and optional type parameters. The body is lazy to
    * break cyclic references.
    */
  final case class Constructor private[Descriptor] (
      id: Identifier,
      typeParams: SList[TypeVarName],
      private val body: Lazy,
  ) extends Descriptor {
    override def hashCode(): Int =
      MurmurHash3.finalizeHash(MurmurHash3.mix(id.hashCode(), typeParams.hashCode()), 2)
    override def equals(obj: Any): Boolean = obj match
      case other: Constructor => this.eq(other) || id == other.id && typeParams == other.typeParams
      case _ => false
  }
  def constructor(id: Identifier, body: => Adt): Constructor =
    constructor(id, Seq.empty, body)
  def constructor(id: Identifier, typeParams: Array[String], body: => Adt): Constructor =
    constructor(id, typeParams.toList, body)
  def constructor(id: Identifier, typeParams: IterableOnce[String], body: => Adt): Constructor =
    Constructor(id, typeParams.iterator.map(TypeVarName).toList, Lazy(body))
  object Constructor:
    def unapply(c: Descriptor.Constructor): (Identifier, SList[TypeVarName], Adt) =
      (c.id, c.typeParams, c.body.value)
    object Applied:
      def unapply(
          d: Application | Constructor | Adt
      ): (Identifier, SList[(TypeVarName, Descriptor)], Adt) = d match
        case Application(Constructor(id, typeParams, body), typeArgs) =>
          (id, typeParams zip typeArgs, body)
        case Constructor(id, typeParams, body) => (id, SList.empty, body)
        case body: Adt => throw Exception(s"Unexpected Adt without Constructor $body")

  /** Type application */
  final case class Application private[Descriptor] (ctor: Constructor, args: SList[Descriptor])
      extends Descriptor
  @varargs def application(ctor: Constructor, arg: Descriptor, args: Descriptor*): Application =
    application(ctor, arg +: args)
  def application(ctor: Constructor, args: Seq[Descriptor]): Application =
    Application(ctor, args.toList)

  final case class Variable private[Descriptor] (name: TypeVarName) extends Descriptor
  def variable(name: String): Variable = Variable(TypeVarName(name))

  /** Utility to handle cyclic references */
  private object Lazy:
    private[Descriptor] def apply(compute: => Adt) = new Lazy(compute)
  private final class Lazy private[Descriptor] (compute: => Adt) extends Serializable {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var _value: Adt = uninitialized
    def value: Adt =
      if _value == null then synchronized(if _value == null then _value = compute)
      _value
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def collect[B](descriptors: IterableOnce[Descriptor])(f: PartialFunction[Descriptor, B]): Seq[B] =
    val result = mutable.Buffer.empty[B]
    val visited = mutable.Set.empty[Descriptor]
    val queue = mutable.Queue.from(descriptors)
    val liftedF = f.lift
    while queue.nonEmpty do
      val current = queue.dequeue()
      if !visited.contains(current) then
        visited.add(current): Unit
        liftedF(current).foreach(result += _)
        current match // enqueue other references
          case Constructor(id, typeParams, body) => queue.addOne(body)
          case Application(ctor, args) => queue.addOne(ctor); queue.addAll(args)
          case Record(fields) => queue.addAll(fields.map(_._2))
          case Variant(cases) => queue.addAll(cases.map(_._2))
          case List(value) => queue.addOne(value)
          case Optional(value) => queue.addOne(value)
          case TextMap(value) => queue.addOne(value)
          case GenMap(key, value) => queue.addOne(key); queue.addOne(value)
          case ContractId(value) => queue.addOne(value)
          case _ => // do nothing
    result.toSeq

  def flatten(descriptors: IterableOnce[Descriptor]): Seq[Descriptor] =
    collect(descriptors)(x => x)
