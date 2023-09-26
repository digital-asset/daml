// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{Util => AstUtil}
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

import scala.annotation.tailrec

private[lf] final class ValueTranslator(
    pkgInterface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
) {

  import Preprocessor._

  @throws[Error.Preprocessing.Error]
  private def labeledRecordToMap(
      fields: ImmArray[(Option[String], Value)]
  ): Option[Map[String, Value]] = {
    @tailrec
    def go(
        fields: FrontStack[(Option[String], Value)],
        map: Map[String, Value],
    ): Option[Map[String, Value]] = {
      fields.pop match {
        case None => Some(map)
        case Some(((None, _), _)) => None
        case Some(((Some(label), value), tail)) =>
          go(tail, map + (label -> value))
      }
    }

    go(fields.toFrontStack, Map.empty)
  }

  val validateCid: ContractId => Unit =
    if (requireV1ContractIdSuffix) { case cid: ContractId.V1 =>
      if (cid.suffix.isEmpty)
        throw Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(cid)
    }
    else { _ => () }

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafeTranslateCid(cid: ContractId): SValue.SContractId = {
    validateCid(cid)
    SValue.SContractId(cid)
  }

  // For efficient reason we do not produce here the monad Result[SValue] but rather throw
  // exception in case of error or package missing.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafeTranslateValue(
      ty: Type,
      value: Value,
      allowCompatibilityTransformations: Boolean = false,
  ): SValue = {

    def go(ty0: Type, value0: Value, nesting: Int = 0): SValue =
      if (nesting > Value.MAXIMUM_NESTING) {
        throw Error.Preprocessing.ValueNesting(value)
      } else {
        val newNesting = nesting + 1
        def typeError(msg: String = s"mismatching type: ${ty0.pretty} and value: $value0") =
          throw Error.Preprocessing.TypeMismatch(ty0, value0, msg)
        val (ty1, tyArgs) = AstUtil.destructApp(ty0)
        ty1 match {
          case TBuiltin(bt) =>
            tyArgs match {
              case Nil =>
                (bt, value0) match {
                  case (BTUnit, ValueUnit) =>
                    SValue.SUnit
                  case (BTBool, ValueBool(b)) =>
                    if (b) SValue.SValue.True else SValue.SValue.False
                  case (BTInt64, ValueInt64(i)) =>
                    SValue.SInt64(i)
                  case (BTTimestamp, ValueTimestamp(t)) =>
                    SValue.STimestamp(t)
                  case (BTDate, ValueDate(t)) =>
                    SValue.SDate(t)
                  case (BTText, ValueText(t)) =>
                    SValue.SText(t)
                  case (BTParty, ValueParty(p)) =>
                    SValue.SParty(p)
                  case _ =>
                    typeError()
                }
              case typeArg0 :: Nil =>
                (bt, value0) match {
                  case (BTNumeric, ValueNumeric(d)) =>
                    typeArg0 match {
                      case TNat(s) =>
                        Numeric.fromBigDecimal(s, d) match {
                          case Right(value) => SValue.SNumeric(value)
                          case Left(message) => typeError(message)
                        }
                      case _ =>
                        typeError()
                    }
                  case (BTContractId, ValueContractId(c)) =>
                    unsafeTranslateCid(c)
                  case (BTOptional, ValueOptional(mbValue)) =>
                    mbValue match {
                      case Some(v) =>
                        SValue.SOptional(Some(go(typeArg0, v, newNesting)))
                      case None =>
                        SValue.SValue.None
                    }
                  case (BTList, ValueList(ls)) =>
                    if (ls.isEmpty) {
                      SValue.SValue.EmptyList
                    } else {
                      SValue.SList(ls.map(go(typeArg0, _, newNesting)))
                    }
                  // textMap
                  case (BTTextMap, ValueTextMap(entries)) =>
                    if (entries.isEmpty) {
                      SValue.SValue.EmptyTextMap
                    } else {
                      SValue.SMap(
                        isTextMap = true,
                        entries = entries.iterator.map { case (k, v) =>
                          SValue.SText(k) -> go(typeArg0, v, newNesting)
                        },
                      )
                    }
                  case _ =>
                    typeError()
                }
              case typeArg0 :: typeArg1 :: Nil =>
                (bt, value0) match {
                  case (BTGenMap, ValueGenMap(entries)) =>
                    if (entries.isEmpty) {
                      SValue.SValue.EmptyGenMap
                    } else {
                      SValue.SMap(
                        isTextMap = false,
                        entries = entries.iterator.map { case (k, v) =>
                          go(typeArg0, k, newNesting) -> go(typeArg1, v, newNesting)
                        },
                      )
                    }
                  case _ =>
                    typeError()
                }
              case _ =>
                typeError()
            }
          case TTyCon(tyCon) =>
            value0 match {
              // variant
              case ValueVariant(mbId, constructorName, val0) =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    typeError(
                      s"Mismatching variant id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val info = handleLookup(
                  pkgInterface.lookupVariantConstructor(tyCon, constructorName)
                )
                val replacedTyp = info.concreteType(tyArgs)
                SValue.SVariant(
                  tyCon,
                  constructorName,
                  info.rank,
                  go(replacedTyp, val0, newNesting),
                )
              // records
              case ValueRecord(mbId, flds) =>
                // Loose protection, as id might not be provided
                mbId.foreach(id =>
                  if (!allowCompatibilityTransformations && id != tyCon)
                    typeError(
                      s"Mismatching record id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val lookupResult = handleLookup(pkgInterface.lookupDataRecord(tyCon))
                val recordFlds = lookupResult.dataRecord.fields

                // TODO: Bring this behaviour back
                // note that we check the number of fields _before_ checking if we can do
                // field reordering by looking at the labels. this means that it's forbidden to
                // repeat keys even if we provide all the labels, which might be surprising
                // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
                // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
                // if (recordFlds.length != flds.length) {
                //   typeError(
                //     s"Expecting ${recordFlds.length} field for record $tyCon, but got ${flds.length}"
                //   )
                // }

                // Steps:
                // insist all values present with no type are set to None
                // insist all missing values have a type of Optional something, and set to None
                //   insist all missing values are at the end of the type

                // Code that takes flds and makes List[mbLbl -> v] but checks nothing
                //   if flds is labelled, it first puts the expected fields in the correct order, then adds everything else in original order
                // then the logic is - is this result is greater in length than types, all extras must be None and are dropped
                // if this result is smaller than types, all extra types must be Optional and result is backfilled

                // TODO: consider using mutable arrays

                val subst = lookupResult.subst(tyArgs)

                val orderedFlds = labeledRecordToMap(flds) match {
                  // Not fully labelled, so we cannot reorder
                  case None => flds
                  case Some(labeledFlds) => {
                    // If fully labelled, we put the fields we know of first (in the correct order), then the rest after
                    // State is (ordered fields, remaining source fields, is taking known labels)
                    val initialState: (Seq[(Option[String], Value)], Map[String, Value], Boolean) =
                      (Seq(), labeledFlds, true)

                    val (backwardsOrderedRecordFlds, remainingLabeledFlds, _) =
                      recordFlds.foldLeft(initialState) {
                        // Taking known labels from the type, order retained but backwards, since we need to carry information forwards
                        case ((fs, labeledFlds, true), (lbl, _)) =>
                          labeledFlds.get(lbl).fold((fs, labeledFlds, false)) { fld =>
                            ((Some(lbl) -> fld) +: fs, labeledFlds - lbl, true)
                          }
                        // Taking unknown labels from the type, note that all unknown labels must be together at the end, so finding
                        // a known label at this point means the record is malformed.
                        // type error here means that whatever missing field caused this switch is genuinely missing
                        case ((fs, labeledFlds, false), (lbl, _)) =>
                          labeledFlds.get(lbl).fold(typeError("bad!")) { _ =>
                            (fs, labeledFlds, false)
                          }
                      }
                    // TODO: This can probably be faster
                    ImmArray.from(
                      backwardsOrderedRecordFlds.reverse ++ remainingLabeledFlds.toSeq.map {
                        case (lbl, v) => (Some(lbl), v)
                      }
                    )
                  }
                }

                val numS = orderedFlds.length
                val numT = recordFlds.length

                val fields = (recordFlds zip orderedFlds).map { case ((lbl, typ), (mbLbl, v)) =>
                  mbLbl.foreach(lbl_ =>
                    if (lbl_ != lbl)
                      typeError(
                        s"Mismatching record field label '$lbl_' (expecting '$lbl') for record $tyCon"
                      )
                  )
                  val replacedTyp = AstUtil.substitute(typ, subst)
                  lbl -> go(replacedTyp, v, newNesting)
                }

                val upgradedFields =
                  if (numS == numT)
                    fields
                  else if (allowCompatibilityTransformations) {
                    if (numS > numT) {
                      // We have additional fields, orderedFlds[numT .. numS - 1] must be None
                      // Thus we are downgrading
                      orderedFlds.strictSlice(numT, numS).foreach {
                        case (_, ValueOptional(None)) =>
                        case (_, ValueOptional(Some(_))) =>
                          typeError(
                            "An optional contract field with a value of Some may not be dropped during downgrading."
                          )
                        case _ =>
                          typeError(
                            "Unexpected non-optional extra contract field encountered during downgrading: something is very wrong."
                          )
                      }
                      fields
                    } else {
                      // We are missing fields, recordFlds[NumS .. NumT - 1] must be Option, then we fill with None
                      // Thus we are upgrading
                      fields slowAppend recordFlds.strictSlice(numS, numT).map {
                        case (lbl, TApp(TBuiltin(BTOptional), _)) => lbl -> SValue.SOptional(None)
                        case _ =>
                          typeError(
                            "Unexpected non-optional extra template field type encountered during upgrading: something is very wrong."
                          )
                      }
                    }
                  } else {
                    if (numS > numT)
                      typeError("report numT - numS extra fields in record")
                    else
                      typeError("report recordFlds(numS) as missing")
                  }

                SValue.SRecord(
                  tyCon,
                  upgradedFields.map(_._1),
                  upgradedFields.iterator.map(_._2).to(ArrayList),
                )

              case ValueEnum(mbId, constructor) if tyArgs.isEmpty =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    typeError(
                      s"Mismatching enum id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val rank = handleLookup(pkgInterface.lookupEnumConstructor(tyCon, constructor))
                SValue.SEnum(tyCon, constructor, rank)
              case _ =>
                typeError()
            }
          case _ =>
            typeError()
        }
      }

    go(ty, value)
  }

  // This does not try to pull missing packages, return an error instead.
  def translateValue(
      ty: Type,
      value: Value,
      allowCompatibilityTransformations: Boolean = false,
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(unsafeTranslateValue(ty, value, allowCompatibilityTransformations))

}
