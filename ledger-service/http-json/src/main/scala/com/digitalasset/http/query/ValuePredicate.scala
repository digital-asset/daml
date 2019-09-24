// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package query

import util.IdentifierConverters.lfIdentifier

import com.digitalasset.daml.lf.data.{ImmArray, Numeric, Ref, SortedLookupList, Time}
import ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.ScalazEqual._
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.value.{Value => V}
import iface.{Type => Ty}
import scalaz.\&/
import scalaz.syntax.std.option._
import scalaz.syntax.std.string._
import spray.json._

sealed abstract class ValuePredicate extends Product with Serializable {
  import ValuePredicate._
  def toFunPredicate: LfV => Boolean = {
    def go(self: ValuePredicate): LfV => Boolean = self match {
      case Literal(p) => p.isDefinedAt

      case RecordSubset(q) =>
        val cq = q map (_ map { case (_, vp) => go(vp) });
        {
          case V.ValueRecord(_, fields) =>
            cq zip fields.toSeq forall {
              case (None, _) => true
              case (Some(fp), (_, lfv)) => fp(lfv)
            }
          case _ => false
        }

      case MapMatch(q) =>
        val cq = (q mapValue go).toImmArray;
        {
          case V.ValueMap(v) if cq.length == v.toImmArray.length =>
            // the sort-by-key is the same for cq and v, so if equal, the keys
            // are at equal indices
            cq.iterator zip v.toImmArray.iterator forall {
              case ((qk, qp), (vk, vv)) => qk == vk && qp(vv)
            }
          case _ => false
        }

      case ListMatch(qs) =>
        val cqs = qs map go;
        {
          case V.ValueList(vs) if cqs.length == vs.length =>
            cqs.iterator zip vs.iterator forall {
              case (q, v) => q(v)
            }
          case _ => false
        }

      case VariantMatch((n1, p)) =>
        val cp = go(p);
        {
          case V.ValueVariant(_, n2, v) if n1 == n2 => cp(v)
          case _ => false
        }

      case OptionalMatch(oq) =>
        oq map go cata (csq => { case V.ValueOptional(Some(v)) => csq(v); case _ => false },
        { case V.ValueOptional(None) => true; case _ => false })

      case Range(_, _) => predicateParseError("range not supported yet")
    }
    go(this)
  }
}

object ValuePredicate {
  type TypeLookup = Ref.Identifier => Option[iface.DefDataType.FWT]
  type LfV = V[V.AbsoluteContractId]

  final case class Literal(p: LfV PartialFunction Unit) extends ValuePredicate
  final case class RecordSubset(fields: ImmArraySeq[Option[(Ref.Name, ValuePredicate)]])
      extends ValuePredicate
  final case class MapMatch(elems: SortedLookupList[ValuePredicate]) extends ValuePredicate
  final case class ListMatch(elems: Vector[ValuePredicate]) extends ValuePredicate
  final case class VariantMatch(elem: (Ref.Name, ValuePredicate)) extends ValuePredicate
  final case class OptionalMatch(elem: Option[ValuePredicate]) extends ValuePredicate
  // boolean is whether inclusive (lte vs lt)
  final case class Range(ltgt: (Boolean, LfV) \&/ (Boolean, LfV), typ: Ty) extends ValuePredicate

  private[http] def fromTemplateJsObject(
      it: Map[String, JsValue],
      typ: domain.TemplateId.RequiredPkg,
      defs: TypeLookup): ValuePredicate =
    fromJsObject(it, iface.TypeCon(iface.TypeConName(lfIdentifier(typ)), ImmArraySeq.empty), defs)

  def fromJsObject(it: Map[String, JsValue], typ: iface.Type, defs: TypeLookup): ValuePredicate = {
    type Result = ValuePredicate

    def fromValue(it: JsValue, typ: iface.Type): Result =
      (typ, it).match2 {
        case p @ iface.TypePrim(_, _) => { case _ => fromPrim(it, p) }
        case tc @ iface.TypeCon(iface.TypeConName(id), _) => {
          case _ =>
            val ddt = defs(id).getOrElse(predicateParseError(s"Type $id not found"))
            fromCon(it, id, tc instantiate ddt)
        }
        case iface.TypeNumeric(scale) => {
          case JsString(q) =>
            val nq = Numeric checkWithinBoundsAndRound (scale, BigDecimal(q)) fold (predicateParseError, identity)
            Literal { case V.ValueNumeric(v) if nq == (v setScale scale) => }
          case JsNumber(q) =>
            val nq = Numeric checkWithinBoundsAndRound (scale, q) fold (predicateParseError, identity)
            Literal { case V.ValueNumeric(v) if nq == (v setScale scale) => }
        }
        case iface.TypeVar(_) => predicateParseError("no vars allowed!")
      }(fallback = illTypedQuery(it, typ))

    def fromCon(it: JsValue, id: Ref.Identifier, typ: iface.DataType.FWT): Result =
      (typ, it).match2 {
        case rec @ iface.Record(_) => {
          case JsObject(fields) =>
            fromRecord(fields, id, rec)
        }
        case iface.Variant(fieldTyps) => {
          case JsObject(fields) =>
            fromVariant(fields, id, fieldTyps)
        }
        case e @ iface.Enum(_) => {
          case JsString(s) => fromEnum(s, id, e)
        }
      }(fallback = illTypedQuery(it, id))

    def fromRecord(
        fields: Map[String, JsValue],
        id: Ref.Identifier,
        typ: iface.Record.FWT): Result = {
      val iface.Record(fieldTyps) = typ
      val invalidKeys = fields.keySet diff fieldTyps.iterator.map(_._1).toSet
      if (invalidKeys.nonEmpty)
        predicateParseError(s"$id does not have fields $invalidKeys")
      RecordSubset(fieldTyps map {
        case (fName, fTy) =>
          fields get fName map (fSpec => (fName, fromValue(fSpec, fTy)))
      })
    }

    def fromVariant(
        fields: Map[String, JsValue],
        id: Ref.Identifier,
        fieldTyps: ImmArraySeq[(Ref.Name, Ty)]): Result = fields.toSeq match {
      case Seq((k, v)) =>
        val name = Ref.Name.assertFromString(k)
        val field: Option[(Ref.Name, Ty)] = fieldTyps.find(_._1 == name)
        val fieldP: Option[(Ref.Name, ValuePredicate)] = field.map {
          case (n, t) => (n, fromValue(v, t))
        }
        fieldP.fold(
          predicateParseError(
            s"Cannot locate Variant's (datacon, type) field, id: $id, name: $name")
        )(VariantMatch)

      case _ => predicateParseError(s"Variant must have exactly 1 field, got: $fields, id: $id")
    }

    def fromEnum(it: String, id: Ref.Identifier, typ: iface.Enum): Result =
      if (typ.constructors contains it)
        Literal { case V.ValueEnum(_, v) if it == (v: String) => } else
        predicateParseError(s"$it not a member of the enum $id")

    def fromOptional(it: JsValue, typ: iface.Type): Result =
      (typ, it).match2 {
        case iface.TypePrim(iface.PrimType.Optional, _) => {
          case JsNull => OptionalMatch(None)
          case JsArray(Seq()) => OptionalMatch(Some(fromOptional(JsNull, typ)))
          case JsArray(Seq(elem)) => OptionalMatch(Some(fromValue(elem, typ)))
        }
        case _ => {
          case JsNull => OptionalMatch(None)
          case other => OptionalMatch(Some(fromValue(other, typ)))
        }
      }(fallback = illTypedQuery(it, typ))

    def fromPrim(it: JsValue, typ: iface.TypePrim): Result = {
      import iface.PrimType._
      def soleTypeArg(of: String) = typ.typArgs match {
        case Seq(hd) => hd
        case _ => predicateParseError(s"missing type arg to $of")
      }
      (typ.typ, it).match2 {
        case Bool => { case JsBoolean(q) => Literal { case V.ValueBool(v) if q == v => } }
        case Int64 => {
          case JsNumber(q) if q.isValidLong =>
            val lq = q.toLongExact
            Literal { case V.ValueInt64(v) if lq == (v: Long) => }
          case JsString(q) =>
            val lq: Long = q.parseLong.fold(e => throw e, identity)
            Literal { case V.ValueInt64(v) if lq == (v: Long) => }
        }
        case Text => { case JsString(q) => Literal { case V.ValueText(v) if q == v => } }
        case Date => {
          case JsString(q) =>
            val dq = Time.Date fromString q fold (predicateParseError(_), identity)
            Literal { case V.ValueDate(v) if dq == v => }
        }
        case Timestamp => {
          case JsString(q) =>
            val tq = Time.Timestamp fromString q fold (predicateParseError(_), identity)
            Literal { case V.ValueTimestamp(v) if tq == v => }
        }
        case Party => {
          case JsString(q) => Literal { case V.ValueParty(v) if q == (v: String) => }
        }
        case ContractId => {
          case JsString(q) => Literal { case V.ValueContractId(v) if q == (v.coid: String) => }
        }
        case List => {
          case JsArray(as) =>
            val elemTy = soleTypeArg("List")
            ListMatch(as.map(a => fromValue(a, elemTy)))
        }
        case Unit => { case JsObject(q) if q.isEmpty => Literal { case V.ValueUnit => } }
        case Optional => {
          case q =>
            val elemTy = soleTypeArg("Optional")
            fromOptional(q, elemTy)
        }
        case Map => {
          case JsObject(q) =>
            val elemTy = soleTypeArg("Map")
            MapMatch(SortedLookupList(q) mapValue (fromValue(_, elemTy)))
        }
      }(fallback = illTypedQuery(it, typ))
    }

    (typ match {
      case tc @ iface.TypeCon(iface.TypeConName(id), typArgs) =>
        for {
          dt <- defs(id)
          recTy <- tc instantiate dt match { case r @ iface.Record(_) => Some(r); case _ => None }
        } yield fromRecord(it, id, recTy)
      case _ => None
    }) getOrElse predicateParseError(s"No record type found for $typ")
  }

  private[this] def illTypedQuery(it: JsValue, typ: Any): Nothing =
    predicateParseError(s"$it is not a query that can match type $typ")

  private def predicateParseError(s: String): Nothing =
    sys.error(s)
}
