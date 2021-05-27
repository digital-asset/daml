// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data._
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError.SErrorCrash
import com.daml.lf.value.{Value => V}

import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.collection.immutable.TreeMap
import scala.util.hashing.MurmurHash3

/** Speedy values. These are the value types recognized by the
  * machine. In addition to the usual types present in the LF value,
  * this also contains partially applied functions (SPAP).
  */
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

      case SRecord(id, fields, svalues) =>
        V.ValueRecord(
          Some(id),
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map({ case (fld, sv) => (Some(fld), sv.toValue) })
          ),
        )
      case SVariant(id, variant, _, sv) =>
        V.ValueVariant(Some(id), variant, sv.toValue)
      case SEnum(id, constructor, _) =>
        V.ValueEnum(Some(id), constructor)
      case SList(lst) =>
        V.ValueList(lst.map(_.toValue))
      case SOptional(mbV) =>
        V.ValueOptional(mbV.map(_.toValue))
      case SMap(true, entries) =>
        V.ValueTextMap(SortedLookupList(entries.map {
          case (SText(t), v) => t -> v.toValue
          case (_, _) => throw SErrorCrash("SValue.toValue: TextMap with non text key")
        }))
      case SMap(false, entries) =>
        V.ValueGenMap(entries.view.map { case (k, v) => k.toValue -> v.toValue }.to(ImmArray))
      case SContractId(coid) =>
        V.ValueContractId(coid)
      case SStruct(_, _) =>
        throw SErrorCrash("SValue.toValue: unexpected SStruct")
      case SAny(_, _) =>
        throw SErrorCrash("SValue.toValue: unexpected SAny")
      case SBigNumeric(_) =>
        throw SErrorCrash("SValue.toValue: unexpected SBigNumeric")
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
      case SEnum(_, _, _) | _: SPrimLit | SToken | STNat(_) | STypeRep(_) => this
      case SPAP(prim, args, arity) =>
        val prim2 = prim match {
          case PClosure(label, expr, vars) =>
            PClosure(label, expr, vars.map(_.mapContractId(f)))
          case other => other
        }
        val args2 = mapArrayList(args, _.mapContractId(f))
        SPAP(prim2, args2, arity)
      case SRecord(tycon, fields, values) =>
        SRecord(tycon, fields, mapArrayList(values, v => v.mapContractId(f)))
      case SStruct(fields, values) =>
        SStruct(fields, mapArrayList(values, v => v.mapContractId(f)))
      case SVariant(tycon, variant, rank, value) =>
        SVariant(tycon, variant, rank, value.mapContractId(f))
      case SList(lst) =>
        SList(lst.map(_.mapContractId(f)))
      case SOptional(mbV) =>
        SOptional(mbV.map(_.mapContractId(f)))
      case SMap(isTextMap, value) =>
        SMap(
          isTextMap,
          value.iterator.map { case (k, v) => k.mapContractId(f) -> v.mapContractId(f) },
        )
      case SAny(ty, value) =>
        SAny(ty, value.mapContractId(f))
    }
}

object SValue {

  /** "Primitives" that can be applied. */
  sealed trait Prim
  final case class PBuiltin(b: SBuiltin) extends Prim

  /** A closure consisting of an expression together with the values the
    * expression is closing over.
    * The [[label]] field is only used during profiling. During non-profiling
    * runs it is always set to [[null]].
    * During profiling, whenever a closure whose [[label]] has been set is
    * entered, we write an "open event" with the label and when the closure is
    * left, we write a "close event" with the same label.
    */
  final case class PClosure(label: Profile.Label, expr: SExpr, frame: Array[SValue])
      extends Prim
      with SomeArrayEquals {
    override def toString: String = s"PClosure($expr, ${frame.mkString("[", ",", "]")})"
  }

  /** A partially applied primitive.
    * An SPAP is *never* fully applied. This is asserted on construction.
    */
  final case class SPAP(prim: Prim, actuals: util.ArrayList[SValue], arity: Int) extends SValue {
    if (actuals.size >= arity) {
      throw SErrorCrash(s"SPAP: unexpected actuals.size >= arity")
    }
    override def toString: String =
      s"SPAP($prim, ${actuals.asScala.mkString("[", ",", "]")}, $arity)"
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class SRecord(id: Identifier, fields: ImmArray[Name], values: util.ArrayList[SValue])
      extends SValue

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  // values must be ordered according fieldNames
  final case class SStruct(fieldNames: Struct[Unit], values: util.ArrayList[SValue]) extends SValue

  final case class SVariant(
      id: Identifier,
      variant: VariantConName,
      constructorRank: Int,
      value: SValue,
  ) extends SValue

  final case class SEnum(id: Identifier, constructor: Name, constructorRank: Int) extends SValue

  final case class SOptional(value: Option[SValue]) extends SValue

  final case class SList(list: FrontStack[SValue]) extends SValue

  // We make the constructor private to ensure entries are sorted according `SMap Ordering`
  final case class SMap private (isTextMap: Boolean, entries: TreeMap[SValue, SValue])
      extends SValue
      with NoCopy {

    def insert(key: SValue, value: SValue): SMap =
      SMap(isTextMap, entries.updated(key, value))

    def delete(key: SValue): SMap =
      SMap(isTextMap, entries - key)

  }

  object SMap {
    implicit def `SMap Ordering`: Ordering[SValue] = svalue.Ordering

    @throws[SErrorCrash]
    // crashes if `k` contains type abstraction, function, Partially applied built-in or updates
    def comparable(k: SValue): Unit = {
      `SMap Ordering`.compare(k, k)
      ()
    }

    def apply(isTextMap: Boolean, entries: Iterator[(SValue, SValue)]): SMap = {
      SMap(
        isTextMap,
        implicitly[Factory[(SValue, SValue), TreeMap[SValue, SValue]]].fromSpecific(entries.map {
          case p @ (k, _) => comparable(k); p
        }),
      )
    }

    def apply(isTextMap: Boolean, entries: (SValue, SValue)*): SMap =
      SMap(isTextMap: Boolean, entries.iterator)
  }

  // represents Any And AnyException
  final case class SAny(ty: Type, value: SValue) extends SValue

  object SAnyException {
    def apply(tyCon: Ref.TypeConName, value: SRecord): SAny = SAny(TTyCon(tyCon), value)

    def unapply(any: SAny): Option[SRecord] =
      any match {
        case SAny(TTyCon(tyCon0), record @ SRecord(tyCon1, _, _)) if tyCon0 == tyCon1 =>
          Some(record)
        case _ =>
          None
      }
  }

  object SArithmeticError {
    // The package ID should match the ID of the stable package daml-prim-DA-Exception-ArithmeticError
    // See test compiler/damlc/tests/src/stable-packages.sh
    val tyCon: Ref.TypeConName = Ref.Identifier.assertFromString(
      "f1cf1ff41057ce327248684089b106d0a1f27c2f092d30f663c919addf173981:DA.Exception.ArithmeticError:ArithmeticError"
    )
    val typ: Type = TTyCon(tyCon)
    val fields: ImmArray[Ref.Name] = ImmArray(Ref.Name.assertFromString("message"))
    def apply(builtinName: String, args: ImmArray[String]): SAny = {
      val array = new util.ArrayList[SValue](1)
      array.add(
        SText(s"ArithmeticError while evaluating ($builtinName ${args.iterator.mkString(" ")}).")
      )
      SAny(typ, SRecord(tyCon, fields, array))
    }
    // Assumes excep is properly typed
    def unapply(excep: SAny): Option[SValue] =
      excep match {
        case SAnyException(SRecord(`tyCon`, _, args)) => Some(args.get(0))
        case _ => None
      }
  }

  // Corresponds to a Daml-LF Nat type reified as a Speedy value.
  // It is currently used to track at runtime the scale of the
  // Numeric builtin's arguments/output. Should never be translated
  // back to Daml-LF expressions / values.
  final case class STNat(n: Numeric.Scale) extends SValue

  // NOTE(JM): We are redefining PrimLit here so it can be unified
  // with SValue and we can remove one layer of indirection.
  sealed trait SPrimLit extends SValue with Equals
  final case class SInt64(value: Long) extends SPrimLit
  // TODO https://github.com/digital-asset/daml/issues/8719
  //  try to factorize SNumeric and SBigNumeric
  //  note it seems that scale is relevant in SNumeric but lost in SBigNumeric
  final case class SNumeric(value: Numeric) extends SPrimLit
  object SNumeric {
    def fromBigDecimal(scale: Numeric.Scale, x: java.math.BigDecimal) =
      Numeric.fromBigDecimal(scale, x) match {
        case Right(value) =>
          Right(SNumeric(value))
        case Left(_) =>
          overflowUnderflow
      }
  }
  final class SBigNumeric private (val value: java.math.BigDecimal) extends SPrimLit {
    override def canEqual(that: Any): Boolean = that match {
      case _: SBigNumeric => true
      case _ => false
    }

    override def equals(obj: Any): Boolean = obj match {
      case that: SBigNumeric => this.value == that.value
      case _ => false
    }

    override def hashCode(): Int = MurmurHash3.mix(getClass.hashCode(), value.hashCode())

    override def toString: String = s"SBigNumeric($value)"
  }
  object SBigNumeric {
    val MaxPrecision = 1 << 16
    val MaxScale = MaxPrecision / 2
    val MinScale = -MaxPrecision / 2 + 1

    def unapply(value: SBigNumeric): Some[java.math.BigDecimal] =
      Some(value.value)

    def fromBigDecimal(x: java.math.BigDecimal): Either[String, SBigNumeric] = {
      val norm = x.stripTrailingZeros()
      if (norm.scale <= MaxScale && norm.precision - norm.scale <= MaxScale)
        Right(new SBigNumeric(norm))
      else
        overflowUnderflow
    }

    def fromNumeric(x: Numeric) =
      new SBigNumeric(x.stripTrailingZeros())

    def assertFromBigDecimal(x: java.math.BigDecimal): SBigNumeric =
      data.assertRight(fromBigDecimal(x))

    val Zero: SBigNumeric = new SBigNumeric(java.math.BigDecimal.ZERO)

    def checkScale(s: Long): Either[String, Int] =
      Either.cond(test = s.abs <= MaxScale, right = s.toInt, left = "invalide scale")
  }
  final case class SText(value: String) extends SPrimLit
  final case class STimestamp(value: Time.Timestamp) extends SPrimLit
  final case class SParty(value: Party) extends SPrimLit
  final case class SBool(value: Boolean) extends SPrimLit
  object SBool {
    def apply(value: Boolean): SBool = if (value) SValue.True else SValue.False
  }
  final case object SUnit extends SPrimLit
  final case class SDate(value: Time.Date) extends SPrimLit
  final case class SContractId(value: V.ContractId) extends SPrimLit
  final case class STypeRep(ty: Type) extends SValue
  // The "effect" token for update or scenario builtin functions.
  final case object SToken extends SValue

  object SValue {
    val Unit = SUnit
    val True = new SBool(true)
    val False = new SBool(false)
    val EmptyList = SList(FrontStack.empty)
    val None = SOptional(Option.empty)
    val EmptyTextMap = SMap(true)
    val EmptyGenMap = SMap(false)
    val Token = SToken
  }

  abstract class SValueContainer[X] {
    def apply(value: SValue): X
    val Unit: X = apply(SValue.Unit)
    val True: X = apply(SValue.True)
    val False: X = apply(SValue.False)
    val EmptyList: X = apply(SValue.EmptyList)
    val EmptyTextMap: X = apply(SValue.EmptyTextMap)
    val EmptyGenMap: X = apply(SValue.EmptyGenMap)
    val None: X = apply(SValue.None)
    val Token: X = apply(SValue.Token)
    def bool(b: Boolean) = if (b) True else False
  }

  private val entryFields = Struct.assertFromNameSeq(List(keyFieldName, valueFieldName))

  // we verify the fields are ordered as the `entry` method expects it.
  assert(entryFields.indexOf(keyFieldName) == 0)
  assert(entryFields.indexOf(valueFieldName) == 1)

  private def entry(key: SValue, value: SValue) = {
    val args = new util.ArrayList[SValue](2)
    args.add(key)
    args.add(value)
    SStruct(entryFields, args)
  }

  def toList(entries: TreeMap[SValue, SValue]): SList =
    SList(
      FrontStack(
        entries.iterator
          .map { case (k, v) =>
            entry(k, v)
          }
          .to(ImmArray)
      )
    )

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

  private[this] val overflowUnderflow = Left("overflow/underflow")

}
