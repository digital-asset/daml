// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package query

import util.IdentifierConverters.lfIdentifier

import com.digitalasset.daml.lf.data.{ImmArray, Numeric, Ref, SortedLookupList, Time, Utf8}
import ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.ScalazEqual._
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.value.{Value => V}
import iface.{Type => Ty}
import dbbackend.Queries.{concatFragment, contractColumnName}

import scalaz.{OneAnd, Order, \&/, \/, \/-}
import scalaz.Tags.Conjunction
import scalaz.std.anyVal._
import scalaz.syntax.apply._
import scalaz.syntax.order._
import scalaz.syntax.tag._
import scalaz.syntax.std.option._
import scalaz.syntax.std.string._
import spray.json._
import doobie.Fragment
import doobie.implicits._

sealed abstract class ValuePredicate extends Product with Serializable {
  import ValuePredicate._
  def toFunPredicate: LfV => Boolean = {
    def go(self: ValuePredicate): LfV => Boolean = self match {
      case Literal(p, _) => p.isDefinedAt

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

      case range: Range[a] =>
        implicit val ord: Order[a] = range.ord
        range.project andThen { a =>
          range.ltgt.bifoldMap {
            case (incl, ceil) => Conjunction(if (incl) a <= ceil else a < ceil)
          } { case (incl, floor) => Conjunction(if (incl) a >= floor else a > floor) }.unwrap
        } orElse { case _ => false }
    }
    go(this)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def toSqlWhereClause: Fragment = {
    import dbbackend.Queries.Implicits._ // JsValue support
    def go(path: Fragment, self: ValuePredicate): SqlWhereClause =
      self match {
        case Literal(_, jq) =>
          Vector(path ++ sql" = $jq::jsonb")
        case _ => Vector.empty // TODO other cases
      }

    concatFragment(
      OneAnd(sql"1 = 1", go(contractColumnName, this) map (sq => sql" AND (" ++ sq ++ sql")")))
  }
}

object ValuePredicate {
  type TypeLookup = Ref.Identifier => Option[iface.DefDataType.FWT]
  type LfV = V[V.AbsoluteContractId]
  type SqlWhereClause = Vector[Fragment]

  val AlwaysFails: SqlWhereClause = Vector(sql"1 = 2")

  final case class Literal(p: LfV PartialFunction Unit, jsonbEqualPart: JsValue)
      extends ValuePredicate
  final case class RecordSubset(fields: ImmArraySeq[Option[(Ref.Name, ValuePredicate)]])
      extends ValuePredicate
  final case class MapMatch(elems: SortedLookupList[ValuePredicate]) extends ValuePredicate
  final case class ListMatch(elems: Vector[ValuePredicate]) extends ValuePredicate
  final case class VariantMatch(elem: (Ref.Name, ValuePredicate)) extends ValuePredicate
  final case class OptionalMatch(elem: Option[ValuePredicate]) extends ValuePredicate
  final case class Range[A](ltgt: Boundaries[A], ord: Order[A], project: LfV PartialFunction A)
      extends ValuePredicate

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
        case iface.TypeNumeric(scale) =>
          numericRangeExpr(scale).toQueryParser
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
        Literal({ case V.ValueEnum(_, v) if it == (v: String) => }, JsString(it))
      else
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
        case Bool => { case jq @ JsBoolean(q) => Literal({ case V.ValueBool(v) if q == v => }, jq) }
        case Int64 => Int64RangeExpr.toQueryParser
        case Text => TextRangeExpr.toQueryParser(Order fromScalaOrdering Utf8.Ordering)
        case Date => DateRangeExpr.toQueryParser
        case Timestamp => TimestampRangeExpr.toQueryParser
        case Party => {
          case jq @ JsString(q) => Literal({ case V.ValueParty(v) if q == (v: String) => }, jq)
        }
        case ContractId => {
          case jq @ JsString(q) =>
            Literal({ case V.ValueContractId(v) if q == (v.coid: String) => }, jq)
        }
        case List => {
          case JsArray(as) =>
            val elemTy = soleTypeArg("List")
            ListMatch(as.map(a => fromValue(a, elemTy)))
        }
        case Unit => {
          case jq @ JsObject(q) if q.isEmpty =>
            // `jq` is technically @>-ambiguous, but only {} can occur in a Unit-typed
            // position, so the ambiguity never yields incorrect results
            Literal({ case V.ValueUnit => }, jq)
        }
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
        case GenMap =>
          // FIXME https://github.com/digital-asset/daml/issues/2256
          predicateParseError("GenMap not supported")
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

  private[this] val Int64RangeExpr = RangeExpr({
    case JsNumber(q) if q.isValidLong =>
      q.toLongExact
    case JsString(q) =>
      q.parseLong.fold(e => throw e, identity)
  }, { case V.ValueInt64(v) => v })(V.ValueInt64)
  private[this] val TextRangeExpr = RangeExpr({ case JsString(s) => s }, {
    case V.ValueText(v) => v
  })(V.ValueText)
  private[this] val DateRangeExpr = RangeExpr({
    case JsString(q) =>
      Time.Date fromString q fold (predicateParseError(_), identity)
  }, { case V.ValueDate(v) => v })(V.ValueDate)
  private[this] val TimestampRangeExpr = RangeExpr({
    case JsString(q) =>
      Time.Timestamp fromString q fold (predicateParseError(_), identity)
  }, { case V.ValueTimestamp(v) => v })(V.ValueTimestamp)
  private[this] def numericRangeExpr(scale: Numeric.Scale) =
    RangeExpr(
      {
        case JsString(q) =>
          Numeric checkWithinBoundsAndRound (scale, BigDecimal(q)) fold (predicateParseError, identity)
        case JsNumber(q) =>
          Numeric checkWithinBoundsAndRound (scale, q) fold (predicateParseError, identity)
      }, { case V.ValueNumeric(v) => v setScale scale }
    )(qv => V.ValueNumeric(Numeric assertFromBigDecimal (scale, qv)))

  private[this] implicit val `jBD order`: Order[java.math.BigDecimal] =
    Order.fromScalaOrdering

  private[this] type Inclusive = Boolean
  private[this] final val Inclusive = true
  private[this] final val Exclusive = false

  type Boundaries[+A] = (Inclusive, A) \&/ (Inclusive, A)

  private[this] final case class RangeExpr[A](
      scalar: JsValue PartialFunction A,
      lfvScalar: LfV PartialFunction A)(normalized: A => LfV) {
    import RangeExpr._

    private def scalarE(it: JsValue): PredicateParseError \/ A =
      scalar.lift(it) \/> predicateParseError(s"invalid boundary $it")

    object Scalar {
      @inline def unapply(it: JsValue): Option[A] = scalar.lift(it)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def unapply(it: JsValue): Option[PredicateParseError \/ Boundaries[A]] =
      it match {
        case JsObject(fields) if fields.keySet exists keys =>
          def badRangeSyntax(s: String): PredicateParseError \/ Nothing =
            predicateParseError(s"Invalid range query, as $s: $it")

          def side(exK: String, inK: String) = {
            assert(keys(exK) && keys(inK)) // forgot to update 'keys' when changing the keys
            (fields get exK, fields get inK) match {
              case (Some(excl), None) => scalarE(excl) map (a => Some((Exclusive, a)))
              case (None, Some(incl)) => scalarE(incl) map (a => Some((Inclusive, a)))
              case (None, None) => \/-(None)
              case (Some(_), Some(_)) => badRangeSyntax(s"only one of $exK, $inK may be used")
            }
          }

          val strays = fields.keySet diff keys
          Some(
            if (strays.nonEmpty) badRangeSyntax(s"extra invalid keys $strays included")
            else {
              val left = side("%lt", "%lte")
              val right = side("%gt", "%gte")
              import \&/._
              ^(left, right) {
                case (Some(l), Some(r)) => Both(l, r)
                case (Some(l), None) => This(l)
                case (None, Some(r)) => That(r)
                case (None, None) => sys.error("impossible; denied by 'fields.keySet exists keys'")
              }
            })
        case _ => None
      }

    def toLiteral(q: A) = {
      import json.JsonProtocol.LfValueDatabaseCodec.{apiValueToJsValue => dbApiValueToJsValue}
      // we must roundtrip through normalized because e.g. there are several
      // queries that equal 5, but only one of those will be used as the
      // SQL representation (which we compare directly for equality)
      Literal({ case v if lfvScalar.lift(v) contains q => }, dbApiValueToJsValue(normalized(q)))
    }

    def toRange(ltgt: Boundaries[A])(implicit A: Order[A]) = Range(ltgt, A, lfvScalar)

    /** Match both the literal and range query cases. */
    def toQueryParser(implicit A: Order[A]): JsValue PartialFunction ValuePredicate = {
      val Self = this;
      {
        case Scalar(q) => toLiteral(q)
        case Self(eoIor) => eoIor.map(toRange).merge
      }
    }
  }

  private[this] object RangeExpr {
    private val keys = Set("%lt", "%lte", "%gt", "%gte")
  }

  private type PredicateParseError = Nothing

  private[this] def illTypedQuery(it: JsValue, typ: Any): PredicateParseError =
    predicateParseError(s"$it is not a query that can match type $typ")

  private def predicateParseError(s: String): PredicateParseError =
    sys.error(s)
}
