// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import java.util
import com.daml.lf.data._
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SExpr.SExpr
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value.ValueArithmeticError
import com.daml.lf.value.{Value => V}
import com.daml.scalautil.Statement.discard
import com.daml.nameof.NameOf

import scala.collection.IndexedSeqView
import scala.jdk.CollectionConverters._
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.util.hashing.MurmurHash3

/** Speedy values. These are the value types recognized by the
  * machine. In addition to the usual types present in the LF value,
  * this also contains partially applied functions (SPAP).
  */
sealed trait SValue {

  import SValue.{SValue => _, _}

  /** Convert a speedy-value to a value which may not be correctly normalized.
    * And so the resulting value should not be serialized.
    */
  def toUnnormalizedValue: V = {
    toValue(
      normalize = false
    )
  }

  /** Convert a speedy-value to a value normalized according to the LF version.
    */
  def toNormalizedValue(version: TransactionVersion): V = {
    import Ordering.Implicits.infixOrderingOps
    toValue(
      normalize = version >= TransactionVersion.minTypeErasure
    )
  }

  private def toValue(
      normalize: Boolean
  ): V = {

    def maybeEraseTypeInfo[X](x: X): Option[X] =
      if (normalize) {
        None
      } else {
        Some(x)
      }

    def go(v: SValue, maxNesting: Int = V.MAXIMUM_NESTING): V = {
      if (maxNesting < 0)
        throw SError.SErrorDamlException(
          interpretation.Error.Limit(interpretation.Error.Limit.ValueNesting(V.MAXIMUM_NESTING))
        )
      val nextMaxNesting = maxNesting - 1
      v match {
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
            maybeEraseTypeInfo(id),
            (fields.toSeq zip svalues.asScala)
              .map { case (fld, sv) => (maybeEraseTypeInfo(fld), go(sv, nextMaxNesting)) }
              .to(ImmArray),
          )
        case SVariant(id, variant, _, sv) =>
          V.ValueVariant(maybeEraseTypeInfo(id), variant, go(sv, nextMaxNesting))
        case SEnum(id, constructor, _) =>
          V.ValueEnum(maybeEraseTypeInfo(id), constructor)
        case SList(lst) =>
          V.ValueList(lst.map(go(_, nextMaxNesting)))
        case SOptional(mbV) =>
          V.ValueOptional(mbV.map(go(_, nextMaxNesting)))
        case SMap(true, entries) =>
          V.ValueTextMap(SortedLookupList(entries.map {
            case (SText(t), v) => t -> go(v, nextMaxNesting)
            case (_, _) =>
              throw SError.SErrorCrash(
                NameOf.qualifiedNameOfCurrentFunc,
                "SValue.toValue: TextMap with non text key",
              )
          }))
        case SMap(false, entries) =>
          V.ValueGenMap(
            entries.view
              .map { case (k, v) => go(k, nextMaxNesting) -> go(v, nextMaxNesting) }
              .to(ImmArray)
          )
        case SContractId(coid) =>
          V.ValueContractId(coid)
        case _: SStruct | _: SAny | _: SBigNumeric | _: STypeRep | _: STNat | _: SPAP | SToken =>
          throw SError.SErrorCrash(
            NameOf.qualifiedNameOfCurrentFunc,
            s"SValue.toValue: unexpected ${getClass.getSimpleName}",
          )
      }
    }
    go(this)
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
      throw SError.SErrorCrash(
        NameOf.qualifiedNameOfCurrentFunc,
        "SPAP: unexpected actuals.size >= arity",
      )
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

  // We make the constructor private to ensure entries are sorted using `SMap Ordering`
  final case class SMap private (isTextMap: Boolean, entries: TreeMap[SValue, SValue])
      extends SValue
      with NoCopy {

    def insert(key: SValue, value: SValue): SMap =
      SMap(isTextMap, entries.updated(key, value))

    def delete(key: SValue): SMap =
      SMap(isTextMap, entries - key)

    // Similar as scala TreeMap#rangeImpl but includes both bounds
    def range(from: Option[SValue], to: Option[SValue]): SMap = {
      val entries1 = from match {
        case Some(from) => entries.rangeFrom(from)
        case None => entries
      }
      val entries2 = to match {
        case Some(to) => entries1.rangeTo(to)
        case None => entries1
      }
      SMap(isTextMap, entries2)
    }
  }

  object SMap {
    implicit def `SMap Ordering`: Ordering[SValue] = svalue.Ordering

    @throws[SError.SError]
    // crashes if `k` contains type abstraction, function, Partially applied built-in or updates
    def comparable(k: SValue): Unit = {
      discard[Int](`SMap Ordering`.compare(k, k))
    }

    /** Build an SMap from an indexed sequence of SValue key/value pairs.
      *
      * SValue keys are assumed to be in ascending order - hence the SMap's TreeMap will be built in time O(n) using a
      * sorted map specialisation.
      */
    def fromOrderedEntries(
        isTextMap: Boolean,
        entries: IndexedSeqView[(SValue, SValue)],
    ): SMap = {
      require(
        entries
          .foldLeft[(Boolean, Option[SValue])]((true, None)) {
            case ((result, previousKey), (currentKey, _)) =>
              (result && previousKey.forall(`SMap Ordering`.lteq(_, currentKey)), Some(currentKey))
          }
          ._1,
        "The entries are not in descending order",
      )

      val sortedEntryMap: SortedMap[SValue, SValue] = new SortedMap[SValue, SValue] {
        private[this] val encapsulatedSortedMap = SortedMap.from(entries)

        override def iterator: Iterator[(SValue, SValue)] = entries.iterator

        override def size: Int = entries.size

        override def ordering: Ordering[SValue] = `SMap Ordering`

        override def updated[V1 >: SValue](key: SValue, value: V1): SortedMap[SValue, V1] =
          encapsulatedSortedMap.updated(key, value)

        override def removed(key: SValue): SortedMap[SValue, SValue] =
          encapsulatedSortedMap.removed(key)

        override def iteratorFrom(start: SValue): Iterator[(SValue, SValue)] =
          encapsulatedSortedMap.iteratorFrom(start)

        override def keysIteratorFrom(start: SValue): Iterator[SValue] =
          encapsulatedSortedMap.keysIteratorFrom(start)

        override def rangeImpl(
            from: Option[SValue],
            until: Option[SValue],
        ): SortedMap[SValue, SValue] = encapsulatedSortedMap.rangeImpl(from, until)

        override def get(key: SValue): Option[SValue] = encapsulatedSortedMap.get(key)
      }

      SMap(isTextMap, TreeMap.from(sortedEntryMap))
    }

    /** Build an SMap from an iterator over SValue key/value pairs.
      *
      * SValue keys are not assumed to be ordered - hence the SMap will be built in time O(n log(n)).
      */
    def apply(isTextMap: Boolean, entries: Iterator[(SValue, SValue)]): SMap =
      SMap(
        isTextMap,
        entries.map { case p @ (k, _) => comparable(k); p }.to(TreeMap),
      )

    /** Build an SMap from a vararg sequence of SValue key/value pairs.
      *
      * SValue keys are not assumed to be ordered - hence the SMap will be built in time O(n log(n)).
      */
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

  object SAnyContract {
    def apply(tyCon: Ref.TypeConName, record: SRecord): SAny = SAny(TTyCon(tyCon), record)

    def unapply(any: SAny): Option[(TypeConName, SRecord)] =
      any match {
        case SAny(TTyCon(tyCon0), record: SRecord) if record.id == tyCon0 =>
          Some(tyCon0, record)
        case _ =>
          None
      }
  }

  object SArithmeticError {
    val fields: ImmArray[Ref.Name] = ImmArray(ValueArithmeticError.fieldName)
    def apply(builtinName: String, args: ImmArray[String]): SAny = {
      val array = ArrayList.single[SValue](
        SText(s"ArithmeticError while evaluating ($builtinName ${args.iterator.mkString(" ")}).")
      )
      SAny(ValueArithmeticError.typ, SRecord(ValueArithmeticError.tyCon, fields, array))
    }
    // Assumes excep is properly typed
    def unapply(excep: SAny): Option[SValue] =
      excep match {
        case SAnyException(SRecord(ValueArithmeticError.tyCon, _, args)) => Some(args.get(0))
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

  def toList(entries: TreeMap[SValue, SValue]): SList =
    SList(
      entries.view
        .map { case (k, v) => SStruct(entryFields, ArrayList.double(k, v)) }
        .to(FrontStack)
    )

  private[this] val overflowUnderflow = Left("overflow/underflow")

}
