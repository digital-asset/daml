// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import java.util

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash
import com.digitalasset.daml.lf.value.{Value => V}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/** Speedy values. These are the value types recognized by the
  * machine. In addition to the usual types present in the LF value,
  * this also contains partially applied functions (SPAP).
  */

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
  ),
)
sealed trait SValue {

  import SValue._

  def toValue: V[V.ContractId] =
    this match {
      case SInt64(x) => V.ValueInt64(x)
      case SNumeric(x) => V.ValueNumeric(x)
      case SText(x) => V.ValueText(x)
      case STimestamp(x) => V.ValueTimestamp(x)
      case SParty(x) => V.ValueParty(x)
      case SBool(x) => V.ValueBool(x)
      case SUnit => V.ValueUnit
      case SDate(x) => V.ValueDate(x)
      case SStruct(fields, svalues) =>
        V.ValueStruct(
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map { case (fld, sv) => (fld, sv.toValue) },
          ),
        )
      case SRecord(id, fields, svalues) =>
        V.ValueRecord(
          Some(id),
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map({ case (fld, sv) => (Some(fld), sv.toValue) }),
          ),
        )
      case SVariant(id, variant, sv) =>
        V.ValueVariant(Some(id), variant, sv.toValue)
      case SEnum(id, constructor) =>
        V.ValueEnum(Some(id), constructor)
      case SList(lst) =>
        V.ValueList(lst.map(_.toValue))
      case SOptional(mbV) =>
        V.ValueOptional(mbV.map(_.toValue))
      case STextMap(mVal) =>
        V.ValueTextMap(SortedLookupList(mVal).mapValue(_.toValue))
      case SGenMap(values) =>
        V.ValueGenMap(ImmArray(values.map { case (k, v) => k.v.toValue -> v.toValue }))
      case SContractId(coid) =>
        V.ValueContractId(coid)
      case SAny(_, _) =>
        throw SErrorCrash("SValue.toValue: unexpected SAny")
      case STypeRep(_) =>
        throw SErrorCrash("SValue.toValue: unexpected STypeRep")
      case STNat(_) =>
        throw SErrorCrash("SValue.toValue: unexpected STNat")
      case _: SPAP =>
        throw SErrorCrash("SValue.toValue: unexpected SPAP")
      case SToken =>
        throw SErrorCrash("SValue.toValue: unexpected SToken")
    }

  def mapContractId(f: V.ContractId => V.ContractId): SValue =
    this match {
      case SContractId(coid) => SContractId(f(coid))
      case SEnum(_, _) | _: SPrimLit | SToken | STNat(_) | STypeRep(_) => this
      case SPAP(prim, args, arity) =>
        val prim2 = prim match {
          case PClosure(expr, vars) =>
            PClosure(expr, vars.map(_.mapContractId(f)))
          case other => other
        }
        val args2 = mapArrayList(args, _.mapContractId(f))
        SPAP(prim2, args2, arity)
      case SRecord(tycon, fields, values) =>
        SRecord(tycon, fields, mapArrayList(values, v => v.mapContractId(f)))
      case SStruct(fields, values) =>
        SStruct(fields, mapArrayList(values, v => v.mapContractId(f)))
      case SVariant(tycon, variant, value) =>
        SVariant(tycon, variant, value.mapContractId(f))
      case SList(lst) =>
        SList(lst.map(_.mapContractId(f)))
      case SOptional(mbV) =>
        SOptional(mbV.map(_.mapContractId(f)))
      case STextMap(value) =>
        STextMap(value.transform((_, v) => v.mapContractId(f)))
      case SGenMap(value) =>
        SGenMap((InsertOrdMap.empty[SGenMap.Key, SValue] /: value) {
          case (acc, (SGenMap.Key(k), v)) => acc + (SGenMap.Key(k.mapContractId(f)) -> v)
        })
      case SAny(ty, value) =>
        SAny(ty, value.mapContractId(f))
    }
}

object SValue {

  /** "Primitives" that can be applied. */
  sealed trait Prim
  final case class PBuiltin(b: SBuiltin) extends Prim
  final case class PClosure(expr: SExpr, closure: Array[SValue]) extends Prim with SomeArrayEquals {
    override def toString: String = s"PClosure($expr, ${closure.mkString("[", ",", "]")})"
  }

  /** A partially (or fully) applied primitive.
    * This is constructed when an argument is applied. When it becomes fully
    * applied (args.size == arity), the machine will apply the arguments to the primitive.
    * If the primitive is a closure, the arguments are pushed to the environment and the
    * closure body is entered.
    */
  final case class SPAP(prim: Prim, args: util.ArrayList[SValue], arity: Int) extends SValue {
    override def toString: String = s"SPAP($prim, ${args.asScala.mkString("[", ",", "]")}, $arity)"
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class SRecord(id: Identifier, fields: Array[Name], values: util.ArrayList[SValue])
      extends SValue

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class SStruct(fields: Array[Name], values: util.ArrayList[SValue]) extends SValue

  final case class SVariant(id: Identifier, variant: VariantConName, value: SValue) extends SValue

  final case class SEnum(id: Identifier, constructor: Name) extends SValue

  final case class SOptional(value: Option[SValue]) extends SValue

  final case class SList(list: FrontStack[SValue]) extends SValue

  final case class STextMap(textMap: HashMap[String, SValue]) extends SValue

  final case class SGenMap(genMap: InsertOrdMap[SGenMap.Key, SValue]) extends SValue

  object SGenMap {
    case class Key(v: SValue) {
      override val hashCode: Int = svalue.Hasher.hash(v)
      override def equals(obj: Any): Boolean = obj match {
        case Key(v2: SValue) => svalue.Equality.areEqual(v, v2)
        case _ => false
      }
    }

    object Key {
      def fromSValue(v: SValue): Either[String, Key] =
        try {
          Right(Key(v))
        } catch {
          case svalue.Hasher.NonHashableSValue(msg) => Left(msg)
        }
    }
  }

  final case class SAny(ty: Type, value: SValue) extends SValue

  // Corresponds to a DAML-LF Nat type reified as a Speedy value.
  // It is currently used to track at runtime the scale of the
  // Numeric builtin's arguments/output. Should never be translated
  // back to DAML-LF expressions / values.
  final case class STNat(n: Numeric.Scale) extends SValue

  // NOTE(JM): We are redefining PrimLit here so it can be unified
  // with SValue and we can remove one layer of indirection.
  sealed trait SPrimLit extends SValue with Equals
  final case class SInt64(value: Long) extends SPrimLit
  final case class SNumeric(value: Numeric) extends SPrimLit
  final case class SText(value: String) extends SPrimLit
  final case class STimestamp(value: Time.Timestamp) extends SPrimLit
  final case class SParty(value: Party) extends SPrimLit
  final case class SBool(value: Boolean) extends SPrimLit
  final case object SUnit extends SPrimLit
  final case class SDate(value: Time.Date) extends SPrimLit
  final case class SContractId(value: V.ContractId) extends SPrimLit
  final case class STypeRep(ty: Type) extends SValue
  // The "effect" token for update or scenario builtin functions.
  final case object SToken extends SValue

  object SValue {
    val Unit = SUnit
    val True = SBool(true)
    val False = SBool(false)
    val EmptyList = SList(FrontStack.empty)
    val None = SOptional(Option.empty)
    val EmptyMap = STextMap(HashMap.empty)
    val EmptyGenMap = SGenMap(InsertOrdMap.empty)
    val Token = SToken
  }

  abstract class SValueContainer[X] {
    def apply(value: SValue): X
    val Unit: X = apply(SValue.Unit)
    val True: X = apply(SValue.True)
    val False: X = apply(SValue.False)
    val EmptyList: X = apply(SValue.EmptyList)
    val EmptyMap: X = apply(SValue.EmptyMap)
    val EmptyGenMap: X = apply(SValue.EmptyGenMap)
    val None: X = apply(SValue.None)
    val Token: X = apply(SValue.Token)
    def bool(b: Boolean) = if (b) True else False
  }

  def fromValue(value0: V[V.ContractId]): SValue = {
    value0 match {
      case V.ValueList(vs) =>
        SList(vs.map[SValue](fromValue))
      case V.ValueContractId(coid) => SContractId(coid)
      case V.ValueInt64(x) => SInt64(x)
      case V.ValueNumeric(x) => SNumeric(x)
      case V.ValueText(t) => SText(t)
      case V.ValueTimestamp(t) => STimestamp(t)
      case V.ValueParty(p) => SParty(p)
      case V.ValueBool(b) => SBool(b)
      case V.ValueDate(x) => SDate(x)
      case V.ValueUnit => SUnit

      case V.ValueRecord(Some(id), fs) =>
        val fields = Name.Array.ofDim(fs.length)
        val values = new util.ArrayList[SValue](fields.length)
        fs.foreach {
          case (optk, v) =>
            optk match {
              case None =>
                // FIXME(JM): Should this be allowed?
                throw SErrorCrash("SValue.fromValue: record missing field name")
              case Some(k) =>
                fields(values.size) = k
                val _ = values.add(fromValue(v))
            }
        }
        SRecord(id, fields, values)

      case V.ValueRecord(None, _) =>
        throw SErrorCrash("SValue.fromValue: record missing identifier")

      case V.ValueStruct(fs) =>
        val fields = Name.Array.ofDim(fs.length)
        val values = new util.ArrayList[SValue](fields.length)
        fs.foreach {
          case (k, v) =>
            fields(values.size) = k
            val _ = values.add(fromValue(v))
        }
        SStruct(fields, values)

      case V.ValueVariant(None, _variant @ _, _value @ _) =>
        throw SErrorCrash("SValue.fromValue: variant without identifier")

      case V.ValueEnum(None, constructor @ _) =>
        throw SErrorCrash("SValue.fromValue: enum without identifier")

      case V.ValueOptional(mbV) =>
        SOptional(mbV.map(fromValue))

      case V.ValueTextMap(map) =>
        STextMap(map.mapValue(fromValue).toHashMap)

      case V.ValueGenMap(entries) =>
        SGenMap(InsertOrdMap(entries.toSeq.map {
          case (k, v) => SGenMap.Key(fromValue(k)) -> fromValue(v)
        }: _*))

      case V.ValueVariant(Some(id), variant, value) =>
        SVariant(id, variant, fromValue(value))

      case V.ValueEnum(Some(id), constructor) =>
        SEnum(id, constructor)
    }
  }

  private def mapArrayList(
      as: util.ArrayList[SValue],
      f: SValue => SValue,
  ): util.ArrayList[SValue] = {
    val bs = new util.ArrayList[SValue](as.size)
    as.forEach { a =>
      val _ = bs.add(f(a))
    }
    bs
  }

}
