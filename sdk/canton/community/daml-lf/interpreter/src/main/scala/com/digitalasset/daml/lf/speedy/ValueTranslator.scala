// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.Error.Upgrade.TranslationFailed
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{LookupError, TypeDestructor}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._

import scala.collection.immutable.ArraySeq

private[lf] final class ValueTranslator(
    pkgInterface: language.PackageInterface,
    forbidLocalContractIds: Boolean,
    forbidTrailingNones: Boolean,
) {

  @throws[TranslationFailed.Error]
  private[this] def handleLookup[X](either: Either[LookupError, X]): X = either match {
    case Right(v) => v
    case Left(error) =>
      throw TranslationFailed.LookupError(error)
  }

  val validateCid: ContractId => Unit =
    if (forbidLocalContractIds) {
      case cid: ContractId.V1 =>
        if (cid.suffix.isEmpty)
          throw TranslationFailed.NonSuffixedV1ContractId(cid)

      case cid: ContractId.V2 =>
        // We forbid only local contract IDs in Engine commands, but not relative contract IDs
        // because relative contract IDs may appear in reinterpretation of projections
        if (cid.suffix.isEmpty)
          throw TranslationFailed.NonSuffixedV2ContractId(cid)
    }
    else { _ => () }

  private[this] def toText(str: String): Text =
    Text.fromString(str) match {
      case Right(value) => value
      case Left(err) => throw TranslationFailed.MalformedText(err)
    }

  @throws[TranslationFailed.Error]
  def unsafeTranslateCid(cid: ContractId): SValue.SContractId = {
    validateCid(cid)
    SValue.SContractId(cid)
  }

  // For efficiency reasons we do not produce here the monad Result[SValue] but rather throw
  // exceptions in case of error or package missing.
  @throws[TranslationFailed.Error]
  def unsafeTranslateValue(
      ty: Type,
      value: Value,
  ): SValue = {
    import TypeDestructor.SerializableTypeF._
    val Destructor = TypeDestructor(pkgInterface)

    def go(ty0: Type, value0: Value, nesting: Int): SValue =
      if (nesting > Value.MAXIMUM_NESTING) throw TranslationFailed.ValueNesting
      else {
        val newNesting = nesting + 1
        def typeError(msg: String = s"mismatching type: ${ty0.pretty} and value: $value0") =
          throw TranslationFailed.TypeMismatch(ty0, value0, msg)

        def invalidValueError(msg: String) =
          throw TranslationFailed.InvalidValue(value0, msg)

        def destruct(typ: Type): TypeDestructor.SerializableTypeF[Type] =
          Destructor.destruct(typ) match {
            case Right(value) => value
            case Left(TypeDestructor.Error.TypeError(err)) =>
              typeError(err)
            case Left(TypeDestructor.Error.LookupError(err)) =>
              throw TranslationFailed.LookupError(err)
          }

        (destruct(ty0), value0) match {
          case (UnitF, ValueUnit) =>
            SValue.SUnit
          case (BoolF, ValueBool(b)) =>
            if (b) SValue.SValue.True else SValue.SValue.False
          case (Int64F, ValueInt64(i)) =>
            SValue.SInt64(i)
          case (TimestampF, ValueTimestamp(t)) =>
            SValue.STimestamp(t)
          case (DateF, ValueDate(t)) =>
            SValue.SDate(t)
          case (TextF, ValueText(t)) =>
            SValue.SText(toText(t))
          case (PartyF, ValueParty(p)) =>
            SValue.SParty(p)
          case (NumericF(s), ValueNumeric(d)) =>
            val dScale = Numeric.scale(d)
            if (dScale != s)
              typeError(
                s"Non-normalized Numeric: the type expects scale $s, but the value has scale ${dScale}"
              )
            Numeric.fromBigDecimal(s, d) match {
              case Right(value) => SValue.SNumeric(value)
              case Left(message) => typeError(message)
            }
          case (ContractIdF(_), ValueContractId(c)) =>
            unsafeTranslateCid(c)
          case (OptionalF(a), ValueOptional(mbValue)) =>
            mbValue match {
              case Some(v) =>
                SValue.SOptional(Some(go(a, v, newNesting)))
              case None =>
                SValue.SValue.None
            }
          case (ListF(a), ValueList(ls)) =>
            if (ls.isEmpty) {
              SValue.SValue.EmptyList
            } else {
              SValue.SList(ls.toImmArray.toSeq.map(go(a, _, newNesting)).toImmArray.toFrontStack)
            }
          case (MapF(a, b), ValueGenMap(entries)) =>
            if (entries.isEmpty) {
              SValue.SValue.EmptyGenMap
            } else {
              // we need to evaluate sentries before calling [[SValue.SMap.fromStrictlyOrderedEntries]] because it can
              // throw RuntimeExceptions unrelated to the ordering of keys, e.g. translation errors. Therefore, sentries
              // cannot be a lazy view.
              val sentries = entries.toSeq.map { case (k, v) =>
                go(a, k, newNesting) -> go(b, v, newNesting)
              }
              try {
                SValue.SMap.fromStrictlyOrderedEntries(
                  isTextMap = false,
                  entries = sentries,
                )
              } catch {
                case _: RuntimeException =>
                  throw TranslationFailed.InvalidValue(
                    value0,
                    s"Unexpected non-strictly ordered GenMap value",
                  )
              }
            }
          case (TextMapF(a), ValueTextMap(entries)) =>
            if (entries.isEmpty) {
              SValue.SValue.EmptyTextMap
            } else {
              SValue.SMap(
                isTextMap = true,
                entries = entries.toImmArray.toSeq.view.map { case (k, v) =>
                  SValue.SText(toText(k)) -> go(a, v, newNesting)
                }.toList,
              )
            }
          case (
                vF @ VariantF(tyCon, _, _, consTyp),
                ValueVariant(mbId, constructorName, val0),
              ) =>
            if (mbId.isDefined)
              invalidValueError(s"Unexpected type id ${mbId.get} in variant value.")
            val i = handleLookup(vF.consRank(constructorName))
            SValue.SVariant(
              tyCon,
              constructorName,
              i,
              go(consTyp(i), val0, newNesting),
            )
          // records
          case (
                RecordF(tyCon, _, fieldNames, fieldTypes),
                ValueRecord(mbId, sourceElements),
              ) =>
            // Fail if the record ID or any label is present in the record value.
            if (mbId.isDefined)
              invalidValueError(s"Unexpected type id ${mbId.get} in record value.")
            sourceElements.foreach { case (mbLabel, _) =>
              mbLabel.foreach(label =>
                invalidValueError(s"Unexpected label ${label} in record value.")
              )
            }
            if (forbidTrailingNones && sourceElements.toSeq.lastOption.exists(_._2 == ValueNone)) {
              invalidValueError("Unexpected trailing None in record.")
            }

            // This code implements the compatibility transformation used for up/down-grading
            // And handles the cases:
            // - UPGRADE:   numT > numS : creates a None for each missing field.
            // - DOWNGRADE: numS > numT : drops each extra field, ensuring it is None.
            // When numS == numT, we won't hit the code marked either as UPGRADE or DOWNGRADE.
            val numS: Int = sourceElements.length
            val numT: Int = fieldTypes.length

            // traverse the sourceElements, getting the corresponding target type
            // when there is no corresponding type, we must be downgrading, and so we insist the value is None
            val values0: List[SValue] =
              sourceElements.toSeq.view.zipWithIndex.flatMap { case ((_, v), i) =>
                fieldTypes.lift(i) match {
                  case Some(targetFieldType) =>
                    val sv: SValue = go(targetFieldType, v, newNesting)
                    List(sv)
                  case None => // DOWNGRADE
                    // i ranges from 0 to numS-1. So i >= numT implies numS > numT
                    assert((numS > i) && (i >= numT))
                    v match {
                      case Value.ValueOptional(None) =>
                        List.empty // ok, drop
                      case Value.ValueOptional(Some(_)) =>
                        typeError(
                          s"Found an optional contract field with a value of Some at index $i, may not be dropped during downgrading."
                        )
                      case _ =>
                        typeError(
                          s"Found non-optional extra field at index $i, cannot remove for downgrading."
                        )
                    }
                }
              }.toList

            val values: ArraySeq[SValue] = {
              if (numT > numS) {
                // UPGRADE
                fieldTypes.view.drop(numS).map(Destructor.destruct(_)).foreach {
                  case Right(OptionalF(_)) =>
                  case _ =>
                    typeError(
                      "Unexpected non-optional extra template field type encountered during upgrading."
                    )
                }
                values0.padTo(numT, SValue.SValue.None)
              } else {
                values0
              }
            }.to(ArraySeq)

            SValue.SRecord(tyCon, fieldNames.to(ImmArray), values)
          case (eF @ EnumF(tyCon, _, _), ValueEnum(mbId, constructor)) =>
            if (mbId.isDefined)
              invalidValueError(s"Unexpected type id ${mbId.get} in enum value.")
            val rank = handleLookup(eF.consRank(constructor))
            SValue.SEnum(tyCon, constructor, rank)
          case _ =>
            typeError()
        }
      }

    go(ty, value, 0)
  }

  def translateValue(
      ty: Type,
      value: Value,
  ): Either[TranslationFailed.Error, SValue] =
    try {
      Right(unsafeTranslateValue(ty, value))
    } catch {
      case e: TranslationFailed.Error =>
        Left(e)
    }
}
