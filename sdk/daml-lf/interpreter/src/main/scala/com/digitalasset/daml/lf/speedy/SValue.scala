// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import java.util
import com.digitalasset.daml.lf.data.{TreeMap => _, _}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SExpr.SExpr
import com.digitalasset.daml.lf.transaction.TransactionVersion
import com.digitalasset.daml.lf.value.Value.ValueArithmeticError
import com.digitalasset.daml.lf.value.{Value => V}
import com.daml.scalautil.Statement.discard
import com.daml.nameof.NameOf

import scala.jdk.CollectionConverters._
import scala.collection.immutable.TreeMap
import scala.util.hashing.MurmurHash3

/** Speedy values. These are the value types recognized by the
  * machine. In addition to the usual types present in the LF value,
  * this also contains partially applied functions (SPAP).
  */
sealed abstract class SValue {

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
  @scala.annotation.nowarn("cat=unused")
  def toNormalizedValue(version: TransactionVersion): V =
    toValue(normalize = true)

  private[this] def toValue(normalize: Boolean): V = {

    def maybeEraseTypeInfo[X](x: X): Option[X] =
      if (normalize) {
        None
      } else {
        Some(x)
      }

    def go(v: SValue, maxNesting: Int = V.MAXIMUM_NESTING): V = {
      if (maxNesting < 0)
        throw SError.SErrorDamlException(
          interpretation.Error.ValueNesting(V.MAXIMUM_NESTING)
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
        case r: SRecord =>
          V.ValueRecord(
            maybeEraseTypeInfo(r.id),
            r.fields.toSeq.zipWithIndex
              .map { case (field, fieldNum) =>
                val sv = r.lookupField(fieldNum, field)
                (maybeEraseTypeInfo(field), go(sv, nextMaxNesting))
              }
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
        case _: SStruct | _: SAny | _: SBigNumeric | _: STypeRep | _: SPAP | SToken =>
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
  sealed abstract class Prim
  final case class PBuiltin(b: SBuiltinFun) extends Prim

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
        NameOf.qualifiedNameOf(SPAP),
        "SPAP: unexpected actuals.size >= arity",
      )
    }

    override def toString: String =
      s"SPAP($prim, ${actuals.asScala.mkString("[", ",", "]")}, $arity)"
  }

  /** We split SRecord (interface) from SRecordRep (implementation/representation)
    *
    * The interface supports creation from separate arrays of fields and values
    * This is used throughout test code.
    *
    * The interface also supports (via unapply) the (legacy) reverse deconstruction.
    * This is used by daml-script.
    *
    * The implementation/representation is via a scala Map.
    * This representation is simple to manipulate (lookupField/updateField).
    *
    * This representation makes illegal cases unrepresentable -- i.e.
    * - mismatched counts of fields/values
    * - repeated field names
    * And prevent brittle access to element values via indexing.
    *
    * Also note updateField has logarithmic complexity (where N is the number of fields)
    * rather than the linear complexity that an array of element values would entail.
    *
    * The representation also includes an ordered field list.
    * This is needed to support the legacy interface used by daml-script.
    * And is also used when we convert the svalue back to a normalised LF value.
    */

  sealed abstract class SRecord extends SValue {
    def id: Identifier
    def fields: ImmArray[Name]
    def values: util.ArrayList[SValue]
    def lookupField(fieldNum: Int, field: Name): SValue
    def updateField(fieldNum: Int, field: Name, value: SValue): SRecord
  }

  object SRecord {
    def apply(id: Identifier, fields: ImmArray[Name], values: util.ArrayList[SValue]): SRecord = {
      if (fields.length != values.size) {
        throw SError.SErrorCrash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"SRecord.apply(#fields=${fields.length}; #values=${values.size}: mismatch!\n- fields=${fields}\n- values=${values}",
        )
      }
      SRecordRep(id, fields, values)
    }
    def unapply(x: SRecord): Option[(Identifier, ImmArray[Name], util.ArrayList[SValue])] = {
      Some((x.id, x.fields, x.values))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class SRecordRep(
      id: Identifier,
      fields: ImmArray[Name],
      values: util.ArrayList[SValue],
  ) extends SRecord {

    def lookupField(fieldNum: Int, field: Name): SValue = {
      values.get(fieldNum)
    }

    def updateField(fieldNum: Int, field: Name, value: SValue): SRecord = {
      val values2: util.ArrayList[SValue] = values.clone.asInstanceOf[util.ArrayList[SValue]]
      discard(values2.set(fieldNum, value))
      this.copy(values = values2)
    }
  }

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
  // and all value are comparable.
  final case class SMap private (isTextMap: Boolean, entries: TreeMap[SValue, SValue])
      extends SValue
      with NoCopy {

    def insert(key: SValue, value: SValue): SMap = {
      SMap.comparable(key)
      SMap(isTextMap, entries.updated(key, value))
    }

    def delete(key: SValue): SMap = {
      SMap.comparable(key)
      SMap(isTextMap, entries - key)
    }

    def get(key: SValue): Option[SValue] = {
      SMap.comparable(key)
      entries.get(key)
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
        entries: Iterable[(SValue, SValue)],
    ): SMap = {
      entries.foreach { case (k, _) => comparable(k) }
      SMap(isTextMap, data.TreeMap.fromOrderedEntries(entries))
    }

    /** Build an SMap from an iterator over SValue key/value pairs.
      *
      * SValue keys are not assumed to be ordered - hence the SMap will be built in time O(n log(n)).
      * If keys are duplicate, the last overrides the firsts
      */
    def apply(isTextMap: Boolean, entries: Iterable[(SValue, SValue)]): SMap = {
      entries.foreach { case (k, _) => comparable(k) }
      SMap(
        isTextMap,
        entries.to(TreeMap),
      )
    }

    /** Build an SMap from a vararg sequence of SValue key/value pairs.
      *
      * SValue keys are not assumed to be ordered - hence the SMap will be built in time O(n log(n)).
      */
    def apply(isTextMap: Boolean, entries: (SValue, SValue)*): SMap =
      SMap(isTextMap: Boolean, entries)
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
    def apply(tyCon: Ref.TypeConName, value: SValue): SAny = {
      value match {
        case record: SRecord =>
          // TODO: https://github.com/digital-asset/daml/issues/17082
          // - investigate CompilerTest failures where this is not true...
          /*if (tyCon != record.id) {
            throw SError.SErrorCrash(
              NameOf.qualifiedNameOfCurrentFunc,
              s"SAnyContract.apply: mismatch tycon, \nA ${tyCon}\nB: ${record.id}",
            )
           }*/
          val _ = tyCon
          SAny(TTyCon(record.id), record)
        case v =>
          throw SError.SErrorCrash(
            NameOf.qualifiedNameOfCurrentFunc,
            s"SAnyContract.apply: expected a record value, got; $v",
          )
      }
    }
  }

  class SArithmeticError(valueArithmeticError: ValueArithmeticError) {
    val fields: ImmArray[Ref.Name] = ImmArray(valueArithmeticError.fieldName)
    def apply(builtinName: String, args: ImmArray[String]): SAny = {
      val array = ArrayList.single[SValue](
        SText(s"ArithmeticError while evaluating ($builtinName ${args.iterator.mkString(" ")}).")
      )
      SAny(valueArithmeticError.typ, SRecord(valueArithmeticError.tyCon, fields, array))
    }
  }

  // NOTE(JM): We are redefining BuiltinLit here so it can be unified
  // with SValue and we can remove one layer of indirection.
  sealed abstract class SBuiltinLit extends SValue with Equals
  final case class SInt64(value: Long) extends SBuiltinLit
  final case class SNumeric(value: Numeric) extends SBuiltinLit
  object SNumeric {
    def fromBigDecimal(scale: Numeric.Scale, x: java.math.BigDecimal) =
      Numeric.fromBigDecimal(scale, x) match {
        case Right(value) =>
          Right(SNumeric(value))
        case Left(_) =>
          overflowUnderflow
      }
  }
  final class SBigNumeric private (val value: java.math.BigDecimal) extends SBuiltinLit {
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
  final case class SText(value: String) extends SBuiltinLit
  final case class STimestamp(value: Time.Timestamp) extends SBuiltinLit
  final case class SParty(value: Party) extends SBuiltinLit
  final case class SBool(value: Boolean) extends SBuiltinLit
  object SBool {
    def apply(value: Boolean): SBool = if (value) SValue.True else SValue.False
  }
  final case object SUnit extends SBuiltinLit
  final case class SDate(value: Time.Date) extends SBuiltinLit
  final case class SContractId(value: V.ContractId) extends SBuiltinLit
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
