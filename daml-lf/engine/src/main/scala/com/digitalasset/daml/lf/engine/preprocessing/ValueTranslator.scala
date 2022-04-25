// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    interface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
) {

  import Preprocessor._

  @throws[Error.Preprocessing.Error]
  private def labeledRecordToMap(
      fields: ImmArray[(Option[String], Value)]
  ): Option[Map[String, Value]] = {
    @tailrec
    def go(
        fields: ImmArray[(Option[String], Value)],
        map: Map[String, Value],
    ): Option[Map[String, Value]] = {
      fields match {
        case ImmArray() => Some(map)
        case ImmArrayCons((None, _), _) => None
        case ImmArrayCons((Some(label), value), tail) =>
          go(tail, map + (label -> value))
      }
    }

    go(fields, Map.empty)
  }

  private[this] val unsafeTranslateV1Cid: ContractId.V1 => SValue.SContractId =
    if (requireV1ContractIdSuffix)
      cid =>
        if (cid.suffix.isEmpty)
          throw Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(cid)
        else
          SValue.SContractId(cid)
    else
      SValue.SContractId(_)

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafeTranslateCid(cid: ContractId): SValue.SContractId =
    cid match {
      case cid1: ContractId.V1 => unsafeTranslateV1Cid(cid1)
    }

  // For efficient reason we do not produce here the monad Result[SValue] but rather throw
  // exception in case of error or package missing.
  @throws[Error.Preprocessing.Error]
  private[preprocessing] def unsafeTranslateValue(
      ty: Type,
      value: Value,
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
                val info = handleLookup(interface.lookupVariantConstructor(tyCon, constructorName))
                val replacedTyp = info.concreteType(tyArgs)
                SValue.SVariant(
                  tyCon,
                  constructorName,
                  info.rank,
                  go(replacedTyp, val0, newNesting),
                )
              // records
              case ValueRecord(mbId, flds) =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    typeError(
                      s"Mismatching record id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val lookupResult = handleLookup(interface.lookupDataRecord(tyCon))
                val recordFlds = lookupResult.dataRecord.fields
                // note that we check the number of fields _before_ checking if we can do
                // field reordering by looking at the labels. this means that it's forbidden to
                // repeat keys even if we provide all the labels, which might be surprising
                // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
                // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
                if (recordFlds.length != flds.length) {
                  typeError(
                    s"Expecting ${recordFlds.length} field for record $tyCon, but got ${flds.length}"
                  )
                }
                val subst = lookupResult.subst(tyArgs)
                val fields = labeledRecordToMap(flds) match {
                  case None =>
                    (recordFlds zip flds).map { case ((lbl, typ), (mbLbl, v)) =>
                      mbLbl.foreach(lbl_ =>
                        if (lbl_ != lbl)
                          typeError(
                            s"Mismatching record field label '$lbl_' (expecting '$lbl') for record $tyCon"
                          )
                      )
                      val replacedTyp = AstUtil.substitute(typ, subst)
                      lbl -> go(replacedTyp, v, newNesting)
                    }
                  case Some(labeledRecords) =>
                    recordFlds.map { case (lbl, typ) =>
                      labeledRecords
                        .get(lbl)
                        .fold(typeError(s"Missing record field '$lbl' for record $tyCon")) { v =>
                          val replacedTyp = AstUtil.substitute(typ, subst)
                          lbl -> go(replacedTyp, v, newNesting)
                        }
                    }
                }
                SValue.SRecord(
                  tyCon,
                  fields.map(_._1),
                  fields.iterator.map(_._2).to(ArrayList),
                )

              case ValueEnum(mbId, constructor) if tyArgs.isEmpty =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    typeError(
                      s"Mismatching enum id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val rank = handleLookup(interface.lookupEnumConstructor(tyCon, constructor))
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
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(unsafeTranslateValue(ty, value))

}
