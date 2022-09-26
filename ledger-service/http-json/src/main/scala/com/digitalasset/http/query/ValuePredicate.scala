// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package query

import util.IdentifierConverters.lfIdentifier
import com.daml.lf.data.{ImmArray, Numeric, Ref, Time, Utf8}
import ImmArray.ImmArraySeq
import com.daml.lf.data.ScalazEqual._
import com.daml.lf.typesig
import com.daml.lf.value.json.JsonVariant
import com.daml.lf.value.{Value => V}
import typesig.{Type => Ty}
import dbbackend.Queries.joinFragment
import json.JsonProtocol.LfValueDatabaseCodec.{apiValueToJsValue => dbApiValueToJsValue}
import scalaz.{OneAnd, Order, \&/, \/, \/-}
import scalaz.Digit._0
import scalaz.Tags.Conjunction
import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.order._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.std.string._
import spray.json._
import doobie.Fragment
import doobie.implicits._
import scalaz.\&/.Both

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

      case VariantMatch((n1, p)) =>
        val cp = go(p);
        {
          case V.ValueVariant(_, n2, v) if n1 == n2 => cp(v)
          case _ => false
        }

      case OptionalMatch(oq) =>
        oq.map(go)
          .cata(
            csq => { case V.ValueOptional(Some(v)) => csq(v); case _ => false },
            {
              case V.ValueOptional(None) => true; case _ => false
            },
          )

      case range: Range[a] =>
        implicit val ord: Order[a] = range.ord
        range.project andThen { a =>
          range.ltgt.bifoldMap { case (incl, ceil) =>
            Conjunction(if (incl) a <= ceil else a < ceil)
          } { case (incl, floor) => Conjunction(if (incl) a >= floor else a > floor) }.unwrap
        } orElse { case _ => false }
    }
    go(this)
  }

  def toSqlWhereClause(implicit sjd: dbbackend.SupportedJdbcDriver.TC): Fragment = {
    import sjd.q.queries.{cmpContractPathToScalar, containsAtContractPath, equalAtContractPath}
    import dbbackend.Queries.{JsonPath => Path}, dbbackend.Queries.OrderOperator._

    object RefName {
      def unapply(s: String): Option[Ref.Name] = Ref.Name.fromString(s).toOption
    }

    final case class Rec(
        raw: SqlWhereClause,
        safe_== : Option[JsValue],
        safe_@> : Option[JsValue],
    ) {
      def flush_@>(path: Path): Option[Fragment] =
        safe_@> map (containsAtContractPath(path, _))
      def flush_==(path: Path): Option[Fragment] =
        safe_== map (equalAtContractPath(path, _))
    }

    def goObject(path: Path, cqs: ImmArraySeq[(String, Rec)], count: Int): Rec = {
      val allSafe_== = cqs collect { case (k, Rec(_, Some(eqv), _)) =>
        (k, eqv)
      }
      val allSafe_@> = cqs collect { case (k, Rec(_, _, Some(ssv))) =>
        (k, ssv)
      }
      // collecting raw, but overriding with an element = if it's =-safe
      // but not @>-safe (equality of @>-safe elements is represented
      // within the safe_@>, which is always collected below)
      val eqOrRaw = cqs map {
        case (RefName(k), r @ Rec(_, Some(_), None)) =>
          r.flush_==(path objectAt k).toList.toVector
        case (_, Rec(raw, _, _)) => raw
      }
      Rec(
        eqOrRaw.toVector.flatten,
        (allSafe_==.length == count) option JsObject(allSafe_== : _*),
        Some(JsObject(allSafe_@> : _*)),
      )
    }

    def go(path: Path, self: ValuePredicate): Rec =
      self match {
        case Literal(_, jq) =>
          Rec(Vector.empty, Some(jq), Some(jq))

        case RecordSubset(qs) =>
          val cqs = qs collect { case Some((k, vp)) => (k, go(path objectAt k, vp)) }
          goObject(path, cqs, qs.length)

        case VariantMatch((dc, q)) =>
          val Rec(vraw, v_==, v_@>) = go(path objectAt JsonVariant.valueKey, q)
          // @> induction is safe because in a variant-typed context, all JsObjects
          // have exactly two keys. @> is conjoined with raw so we use it to add
          // the tag check
          Rec(
            vraw,
            v_== map (JsonVariant(dc, _)),
            v_@> map (JsonVariant(dc, _)) orElse Some(JsonVariant withoutValue dc),
          )

        case OptionalMatch(None) =>
          Rec(Vector.empty, Some(JsNull), Some(JsNull))

        case OptionalMatch(Some(OptionalMatch(None))) =>
          Rec(
            Vector(equalAtContractPath(path, JsArray())),
            Some(JsArray()),
            Some(JsArray()),
          )

        case OptionalMatch(Some(oq @ OptionalMatch(Some(_)))) =>
          val cq = go(path arrayAt _0, oq)
          // we don't do a length check because arrays here have 1 elem at most;
          // [] @> [x] is false for all x
          Rec(cq.raw, cq.safe_== map (JsArray(_)), cq.safe_@> map (JsArray(_)))

        case OptionalMatch(Some(oq)) =>
          go(path, oq)

        case range: Range[a] =>
          range.ltgt match {
            case Both((Inclusive, ceil), (Inclusive, floor)) if range.ord.equal(ceil, floor) =>
              val jsv = dbApiValueToJsValue(range.normalize(ceil))
              Rec(Vector.empty, Some(jsv), Some(jsv))
            case _ =>
              // this output relies on a *big* invariant: comparing the raw JSON data
              // with the built-in SQL operators <, >, &c, yields equal results to
              // comparing the same data in a data-aware way. That's why we *must* use
              // numbers-as-numbers in LfValueDatabaseCodec, and why ISO-8601 strings
              // for dates and timestamps are so important.
              val exprs = range.ltgt
                .umap(
                  _ map (boundary => dbApiValueToJsValue(range.normalize(boundary)))
                )
                .bifoldMap { case (incl, ceil) =>
                  Vector(cmpContractPathToScalar(path, if (incl) LTEQ else LT, ceil))
                } { case (incl, floor) =>
                  Vector(cmpContractPathToScalar(path, if (incl) GTEQ else GT, floor))
                }
              Rec(exprs, None, None)
          }
      }

    val basePath = Path(Vector.empty)
    val outerRec = go(basePath, this)
    outerRec flush_== basePath getOrElse {
      val preds = outerRec.raw ++ outerRec.flush_@>(basePath).toList match {
        case hd +: tl => OneAnd(hd, tl)
        case _ => OneAnd(sql"1 = 1", Vector.empty)
      }
      joinFragment(preds, sql" AND ")
    }
  }
}

object ValuePredicate {
  type TypeLookup = Ref.Identifier => Option[typesig.DefDataType.FWT]
  type LfV = V
  type SqlWhereClause = Vector[Fragment]

  val AlwaysFails: SqlWhereClause = Vector(sql"1 = 2")

  final case class Literal(p: LfV PartialFunction Unit, jsonbEqualPart: JsValue)
      extends ValuePredicate
  final case class RecordSubset(fields: ImmArraySeq[Option[(Ref.Name, ValuePredicate)]])
      extends ValuePredicate
  final case class VariantMatch(elem: (Ref.Name, ValuePredicate)) extends ValuePredicate
  final case class OptionalMatch(elem: Option[ValuePredicate]) extends ValuePredicate
  final case class Range[A](
      ltgt: Boundaries[A],
      ord: Order[A],
      project: LfV PartialFunction A,
      normalize: A => LfV,
  ) extends ValuePredicate

  private[http] def fromTemplateJsObject(
      it: Map[String, JsValue],
      typ: domain.ContractTypeId.RequiredPkg,
      defs: TypeLookup,
  ): ValuePredicate =
    fromJsObject(
      it,
      typesig.TypeCon(typesig.TypeConName(lfIdentifier(typ)), ImmArraySeq.empty),
      defs,
    )

  def fromJsObject(
      it: Map[String, JsValue],
      typ: typesig.Type,
      defs: TypeLookup,
  ): ValuePredicate = {
    type Result = ValuePredicate

    def fromValue(it: JsValue, typ: typesig.Type): Result =
      (typ, it).match2 {
        case p @ typesig.TypePrim(_, _) => { case _ => fromPrim(it, p) }
        case tc @ typesig.TypeCon(typesig.TypeConName(id), _) => { case _ =>
          val ddt = defs(id).getOrElse(predicateParseError(s"Type $id not found"))
          fromCon(it, id, tc instantiate ddt)
        }
        case typesig.TypeNumeric(scale) =>
          numericRangeExpr(scale).toQueryParser
        case typesig.TypeVar(_) => predicateParseError("no vars allowed!")
      }(fallback = illTypedQuery(it, typ))

    def fromCon(it: JsValue, id: Ref.Identifier, typ: typesig.DataType.FWT): Result =
      (typ, it).match2 {
        case rec @ typesig.Record(_) => { case JsObject(fields) =>
          fromRecord(fields, id, rec)
        }
        case typesig.Variant(fieldTyps) => { case JsonVariant(tag, nestedValue) =>
          fromVariant(tag, nestedValue, id, fieldTyps)
        }
        case e @ typesig.Enum(_) => { case JsString(s) =>
          fromEnum(s, id, e)
        }
      }(fallback = illTypedQuery(it, id))

    def fromRecord(
        fields: Map[String, JsValue],
        id: Ref.Identifier,
        typ: typesig.Record.FWT,
    ): Result = {
      val typesig.Record(fieldTyps) = typ
      val invalidKeys = fields.keySet diff fieldTyps.iterator.map(_._1).toSet
      if (invalidKeys.nonEmpty)
        predicateParseError(s"$id does not have fields $invalidKeys")
      RecordSubset(fieldTyps map { case (fName, fTy) =>
        fields get fName map (fSpec => (fName, fromValue(fSpec, fTy)))
      })
    }

    def fromVariant(
        tag: String,
        nestedValue: JsValue,
        id: Ref.Identifier,
        fieldTyps: ImmArraySeq[(Ref.Name, Ty)],
    ): Result = {

      val fieldP: Option[(Ref.Name, ValuePredicate)] =
        fieldTyps.collectFirst {
          case (n, t) if tag == (n: String) =>
            (n, fromValue(nestedValue, t))
        }

      fieldP.fold(
        predicateParseError(s"Cannot locate Variant's (datacon, type) field, id: $id, tag: $tag")
      )(VariantMatch)
    }

    def fromEnum(it: String, id: Ref.Identifier, typ: typesig.Enum): Result =
      if (typ.constructors contains it)
        Literal({ case V.ValueEnum(_, v) if it == (v: String) => }, JsString(it))
      else
        predicateParseError(s"$it not a member of the enum $id")

    def fromOptional(it: JsValue, typ: typesig.Type): Result =
      (typ, it).match2 {
        case typesig.TypePrim(typesig.PrimType.Optional, _) => {
          case JsNull => OptionalMatch(None)
          case JsArray(Seq()) => OptionalMatch(Some(fromOptional(JsNull, typ)))
          case JsArray(Seq(elem)) => OptionalMatch(Some(fromValue(elem, typ)))
        }
        case _ => {
          case JsNull => OptionalMatch(None)
          case other => OptionalMatch(Some(fromValue(other, typ)))
        }
      }(fallback = illTypedQuery(it, typ))

    def fromPrim(it: JsValue, typ: typesig.TypePrim): Result = {
      import typesig.PrimType._
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
        case Party => { case jq @ JsString(q) =>
          Literal({ case V.ValueParty(v) if q == (v: String) => }, jq)
        }
        case ContractId => { case jq @ JsString(q) =>
          Literal({ case V.ValueContractId(v) if q == (v.coid: String) => }, jq)
        }
        case Unit => {
          case jq @ JsObject(q) if q.isEmpty =>
            // `jq` is technically @>-ambiguous, but only {} can occur in a Unit-typed
            // position, so the ambiguity never yields incorrect results
            Literal({ case V.ValueUnit => }, jq)
        }
        case Optional => { case q =>
          val elemTy = soleTypeArg("Optional")
          fromOptional(q, elemTy)
        }
        case List | TextMap | GenMap =>
          predicateParseError(s"${typ.typ} not supported")
      }(fallback = illTypedQuery(it, typ))
    }

    (typ match {
      case tc @ typesig.TypeCon(typesig.TypeConName(id), typArgs @ _) =>
        for {
          dt <- defs(id)
          recTy <- tc instantiate dt match { case r @ typesig.Record(_) => Some(r); case _ => None }
        } yield fromRecord(it, id, recTy)
      case _ => None
    }) getOrElse predicateParseError(s"No record type found for $typ")
  }

  private[this] val Int64RangeExpr = RangeExpr(
    {
      case JsNumber(q) if q.isValidLong =>
        q.toLongExact
      case JsString(q) =>
        q.parseLong.fold(e => throw e, identity)
    },
    { case V.ValueInt64(v) => v },
  )(V.ValueInt64)
  private[this] val TextRangeExpr = RangeExpr(
    { case JsString(s) => s },
    { case V.ValueText(v) =>
      v
    },
  )(V.ValueText)
  private[this] val DateRangeExpr = RangeExpr(
    { case JsString(q) =>
      Time.Date.fromString(q).fold(predicateParseError(_), identity)
    },
    { case V.ValueDate(v) => v },
  )(V.ValueDate)
  private[this] val TimestampRangeExpr = RangeExpr(
    { case JsString(q) =>
      Time.Timestamp.fromString(q).fold(predicateParseError(_), identity)
    },
    { case V.ValueTimestamp(v) => v },
  )(V.ValueTimestamp)
  private[this] def numericRangeExpr(scale: Numeric.Scale) =
    RangeExpr(
      {
        case JsString(q) =>
          Numeric
            .checkWithinBoundsAndRound(
              scale,
              BigDecimal(
                q
              ),
            )
            .fold(predicateParseError, identity)
        case JsNumber(q) =>
          Numeric.checkWithinBoundsAndRound(scale, q).fold(predicateParseError, identity)
      },
      { case V.ValueNumeric(v) => v setScale scale },
    )(qv => V.ValueNumeric(Numeric.assertFromBigDecimal(scale, qv)))

  private[this] implicit val `jBD order`: Order[java.math.BigDecimal] =
    Order.fromScalaOrdering

  private type Inclusive = Boolean
  private final val Inclusive = true
  private final val Exclusive = false

  type Boundaries[A] = (Inclusive, A) \&/ (Inclusive, A)

  private[this] final case class RangeExpr[A](
      scalar: JsValue PartialFunction A,
      lfvScalar: LfV PartialFunction A,
  )(normalized: A => LfV) {
    import RangeExpr._

    private def scalarE(it: JsValue): PredicateParseError \/ A =
      scalar.lift(it) \/> predicateParseError(s"invalid boundary $it")

    object Scalar {
      @inline def unapply(it: JsValue): Option[A] = scalar.lift(it)
    }

    def unapply(it: JsValue): Option[PredicateParseError \/ Boundaries[A]] =
      it match {
        case JsObject(fields) if fields.keySet exists keys =>
          def badRangeSyntax[Bot](s: String): PredicateParseError \/ Bot =
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
            }
          )
        case _ => None
      }

    def toLiteral(q: A) = {
      // we must roundtrip through normalized because e.g. there are several
      // queries that equal 5, but only one of those will be used as the
      // SQL representation (which we compare directly for equality)
      Literal({ case v if lfvScalar.lift(v) contains q => }, dbApiValueToJsValue(normalized(q)))
    }

    def toRange(ltgt: Boundaries[A])(implicit A: Order[A]) = Range(ltgt, A, lfvScalar, normalized)

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
