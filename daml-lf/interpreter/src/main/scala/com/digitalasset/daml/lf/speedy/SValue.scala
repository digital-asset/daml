// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import java.util

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.lfpackage.Ast._
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
    "org.wartremover.warts.Any"
  ))
sealed trait SValue {

  import SValue._

  def toValue: V[V.VContractId] =
    this match {
      case SInt64(x) => V.ValueInt64(x)
      case SDecimal(x) => V.ValueDecimal(x)
      case SText(x) => V.ValueText(x)
      case STimestamp(x) => V.ValueTimestamp(x)
      case SParty(x) => V.ValueParty(x)
      case SBool(x) => V.ValueBool(x)
      case SUnit(_) => V.ValueUnit
      case SDate(x) => V.ValueDate(x)
      case STuple(fields, svalues) =>
        V.ValueTuple(
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map {
                case (fld, sv) =>
                  (fld, sv.toValue)
              }))

      case SRecord(id, fields, svalues) =>
        V.ValueRecord(
          Some(id),
          ImmArray(
            fields.toSeq
              .zip(svalues.asScala)
              .map({ case (fld, sv) => (Some(fld), sv.toValue) })
          )
        )

      case SVariant(id, variant, sv) =>
        V.ValueVariant(Some(id), variant, sv.toValue)
      case SList(lst) =>
        V.ValueList(lst.map(_.toValue))
      case SOptional(mbV) =>
        V.ValueOptional(mbV.map(_.toValue))
      case SMap(mVal) =>
        V.ValueMap(SortedLookupList(mVal).mapValue(_.toValue))
      case SContractId(coid) =>
        V.ValueContractId(coid)

      case _: SPAP =>
        throw SErrorCrash("SValue.toValue: unexpected SPAP")

      case SToken =>
        throw SErrorCrash("SValue.toValue: unexpected SToken")
    }

  private def mapArrayList(
      as: util.ArrayList[SValue],
      f: SValue => SValue): util.ArrayList[SValue] = {
    val bs = new util.ArrayList[SValue](as.size)
    as.forEach { a =>
      val _ = bs.add(f(a))
    }
    bs
  }

  def mapContractId(f: V.VContractId => V.VContractId): SValue =
    this match {
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
      case STuple(fields, values) =>
        STuple(fields, mapArrayList(values, v => v.mapContractId(f)))
      case SVariant(tycon, variant, value) =>
        SVariant(tycon, variant, value.mapContractId(f))
      case SList(lst) =>
        SList(lst.map(_.mapContractId(f)))
      case SOptional(mbV) =>
        SOptional(mbV.map(_.mapContractId(f)))
      case SMap(value) =>
        SMap(value.transform((_, v) => v.mapContractId(f)))
      case SContractId(coid) =>
        SContractId(f(coid))
      case _: SPrimLit => this
      case SToken => this
    }

  def equalTo(v2: SValue): Boolean = {
    (this, v2) match {
      case (_: SPAP, _) => false
      case (_, _: SPAP) => false
      case (SRecord(tycon, _, values), SRecord(tycon2, _, values2)) =>
        tycon == tycon2 &&
          values.iterator.asScala.zip(values2.iterator.asScala).forall {
            case (x, y) => x.equalTo(y)
          }

      case (STuple(fields, values), STuple(fields2, values2)) =>
        ComparableArray(fields) == ComparableArray(fields2) &&
          values.iterator.asScala.zip(values2.iterator.asScala).forall {
            case (x, y) => x.equalTo(y)
          }

      case (SVariant(tycon1, con1, value), SVariant(tycon2, con2, value2)) =>
        tycon1 == tycon2 && con1 == con2 && value.equalTo(value2)
      case (SContractId(coid), SContractId(coid2)) =>
        coid == coid2
      case (SList(lst), SList(lst2)) =>
        lst.iterator.zipAll(lst2.iterator, null, null).forall {
          case (x, y) => x.equalTo(y)
        }
      case (x: SPrimLit, y: SPrimLit) =>
        x == y
      case _ =>
        false
    }
  }
}

object SValue {

  /** "Primitives" that can be applied. */
  sealed trait Prim
  final case class PBuiltin(b: SBuiltin) extends Prim
  final case class PClosure(expr: SExpr, closure: Array[SValue]) extends Prim with SomeArrayEquals

  /** A partially (or fully) applied primitive.
    * This is constructed when an argument is applied. When it becomes fully
    * applied (args.size == arity), the machine will apply the arguments to the primitive.
    * If the primitive is a closure, the arguments are pushed to the environment and the
    * closure body is entered.
    */
  final case class SPAP(prim: Prim, args: util.ArrayList[SValue], arity: Int) extends SValue

  final case class SRecord(id: Identifier, fields: Array[Name], values: util.ArrayList[SValue])
      extends SValue
      with SomeArrayEquals

  final case class STuple(fields: Array[Name], values: util.ArrayList[SValue])
      extends SValue
      with SomeArrayEquals

  final case class SVariant(id: Identifier, variant: VariantConName, value: SValue) extends SValue

  final case class SOptional(value: Option[SValue]) extends SValue

  final case class SList(list: FrontStack[SValue]) extends SValue

  final case class SMap(value: HashMap[String, SValue]) extends SValue

  // NOTE(JM): We are redefining PrimLit here so it can be unified
  // with SValue and we can remove one layer of indirection.
  sealed trait SPrimLit extends SValue with Equals
  final case class SInt64(value: Long) extends SPrimLit
  final case class SDecimal(value: Decimal) extends SPrimLit
  final case class SText(value: String) extends SPrimLit
  final case class STimestamp(value: Time.Timestamp) extends SPrimLit
  final case class SParty(value: Party) extends SPrimLit
  final case class SBool(value: Boolean) extends SPrimLit
  final case class SUnit(value: Unit) extends SPrimLit
  final case class SDate(value: Time.Date) extends SPrimLit
  final case class SContractId(value: V.VContractId) extends SPrimLit

  // The "effect" token for update or scenario builtin functions.
  final case object SToken extends SValue

  def fromValue(value0: V[V.VContractId]): SValue = {
    value0 match {
      case V.ValueList(vs) =>
        SList(vs.map[SValue](fromValue))
      case V.ValueContractId(coid) => SContractId(coid)
      case V.ValueInt64(x) => SInt64(x)
      case V.ValueDecimal(x) => SDecimal(x)
      case V.ValueText(t) => SText(t)
      case V.ValueTimestamp(t) => STimestamp(t)
      case V.ValueParty(p) => SParty(p)
      case V.ValueBool(b) => SBool(b)
      case V.ValueDate(x) => SDate(x)
      case V.ValueUnit => SUnit(())

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

      case V.ValueTuple(fs) =>
        val fields = Name.Array.ofDim(fs.length)
        val values = new util.ArrayList[SValue](fields.length)
        fs.foreach {
          case (k, v) =>
            fields(values.size) = k
            val _ = values.add(fromValue(v))
        }
        STuple(fields, values)

      case V.ValueVariant(None, _variant @ _, _value @ _) =>
        throw SErrorCrash("SValue.fromValue: variant without identifier")

      case V.ValueOptional(mbV) =>
        SOptional(mbV.map(fromValue))

      case V.ValueMap(map) =>
        SMap(map.mapValue(fromValue).toHashMap)

      case V.ValueVariant(Some(id), variant, value) =>
        SVariant(id, variant, fromValue(value))
    }
  }

  private[speedy] val ComparableArray = SomeArrayEquals.ComparableArray
}
