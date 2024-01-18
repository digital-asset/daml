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

  import ValueTranslator._
  import Preprocessor._

  @throws[Error.Preprocessing.Error]
  private def labeledRecordToMap(
      fields: ImmArray[(Option[Ref.Name], Value)]
  ): Either[String, Option[Map[Ref.Name, Value]]] = {
    @tailrec
    def go(
        fields: FrontStack[(Option[Ref.Name], Value)],
        map: Map[Ref.Name, Value],
    ): Either[String, Option[Map[Ref.Name, Value]]] = {
      fields.pop match {
        case None => Right(Some(map))
        case Some(((None, _), _)) => Right(None)
        // Retain error on duplicate label behaviour from pre-upgrades
        case Some(((Some(label), _), _)) if map.contains(label) =>
          Left(s"Duplicate label $label in record")
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
      config: Config,
  ): SValue = {
    // TODO: https://github.com/digital-asset/daml/issues/17082
    //   Should we consider factorizing this code with Seedy.Machine#importValues

    def go(ty0: Type, value0: Value, nesting: Int = 0): SValue =
      if (nesting > Value.MAXIMUM_NESTING) {
        throw Error.Preprocessing.ValueNesting(value)
      } else {
        val newNesting = nesting + 1
        def typeError(msg: String = s"mismatching type: ${ty0.pretty} and value: $value0") =
          throw Error.Preprocessing.TypeMismatch(ty0, value0, msg)
        def checkUserTypeId(targetType: Ref.TypeConName, mbSourceType: Option[Ref.TypeConName]) =
          mbSourceType.foreach(sourceType =>
            if (config.ignorePackageId) {
              // In case of upgrade we simply ignore the package ID.
              if (targetType.qualifiedName != sourceType.qualifiedName)
                typeError(
                  s"Mismatching variant id, the type tells us ${targetType.qualifiedName}, but the value tells us ${sourceType.qualifiedName}"
                )
            } else {
              if (targetType != sourceType)
                typeError(
                  s"Mismatching variant id, the type tells us $targetType, but the value tells us $sourceType"
                )
            }
          )

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
                checkUserTypeId(tyCon, mbId)
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
              case ValueRecord(mbId, sourceElements) =>
                checkUserTypeId(tyCon, mbId)
                val lookupResult = handleLookup(pkgInterface.lookupDataRecord(tyCon))
                val targetFieldsAndTypes = lookupResult.dataRecord.fields
                val subst = lookupResult.subst(tyArgs)

                def addMissingField(lbl: Ref.Name, ty: Type): (Option[Ref.Name], Value) =
                  ty match {
                    // If missing field is optional, fill it with None
                    case TApp(TBuiltin(BTOptional), _) => (Some(lbl), Value.ValueOptional(None))
                    // Else, throw error
                    case _ =>
                      typeError(
                        s"Missing non-optional field \"$lbl\", cannot upgrade non-optional fields."
                      )
                  }

                if (config.enableUpgrade) {
                  val oLabeledFlds =
                    labeledRecordToMap(sourceElements)
                      .fold(typeError, identity _)

                  // correctFields: (correct only by label/position) gives the value and type, length == targetFieldsAndTypes
                  //   filled with Nones when type is Optional
                  // extraFields: Unknown additional fields with name and value
                  val (correctFields, extraFields): (
                      Seq[(Ref.Name, Value, Type)],
                      ImmArray[(Option[Ref.Name], Value)],
                  ) =
                    oLabeledFlds match {
                      // Not fully labelled (or reordering disabled), so order dependent
                      // Additional fields should downgrade, missing fields should upgrade
                      case None => {
                        val correctFields = targetFieldsAndTypes.toSeq.zipWithIndex.map {
                          case ((lbl, ty), i) =>
                            val (mbLbl, v) =
                              sourceElements.get(i).getOrElse(addMissingField(lbl, ty))
                            mbLbl.foreach(lbl_ =>
                              if (lbl_ != lbl)
                                typeError(
                                  s"Mismatching record field label '$lbl_' (expecting '$lbl') for record $tyCon"
                                )
                            )
                            (lbl, v, ty)
                        }
                        val numS = sourceElements.length
                        val numT = targetFieldsAndTypes.length
                        // We have extra fields
                        val extraFields =
                          if (numS > numT)
                            sourceElements.strictSlice(numT, numS)
                          else
                            ImmArray.empty

                        (correctFields, extraFields)
                      }
                      // Fully labelled and allowed to re-order
                      case Some(labeledFlds) => {
                        // new logic
                        // iterate the expected fields, replace any missing with none
                        //   while iterating, remove from remaining

                        val initialState: (Seq[(Ref.Name, Value, Type)], Map[Ref.Name, Value]) =
                          (Seq(), labeledFlds)

                        val (backwardsCorrectFields, remaining) =
                          targetFieldsAndTypes.foldLeft(initialState) {
                            case ((correctFields, remaining), (lbl, ty)) =>
                              val v = remaining.get(lbl).getOrElse(addMissingField(lbl, ty)._2)
                              ((lbl, v, ty) +: correctFields, remaining - lbl)
                          }

                        // Put our fields the correct way around.
                        val correctFields = backwardsCorrectFields.reverse
                        // Wrap additional field names in Some to match with ordered case.
                        val extraFields = ImmArray.from(remaining.toSeq.map { case (lbl, v) =>
                          (Some(lbl), v)
                        })

                        (correctFields, extraFields)
                      }
                    }

                  // Recursive substitution
                  val translatedCorrectFields = correctFields.map { case (lbl, v, typ) =>
                    val replacedTyp = AstUtil.substitute(typ, subst)
                    lbl -> go(replacedTyp, v, newNesting)
                  }

                  extraFields.foreach {
                    // If additional field is None, do nothing
                    case (_, ValueOptional(None)) =>
                    // Else, error depending on type
                    case (oLbl, ValueOptional(Some(_))) =>
                      typeError(
                        s"An optional contract field${oLbl.fold("")(lbl => s" (\"$lbl\")")} with a value of Some may not be dropped during downgrading."
                      )
                    case (oLbl, _) =>
                      typeError(
                        s"Found non-optional extra field${oLbl.fold("")(lbl => s" \"$lbl\"")}, cannot remove for downgrading."
                      )
                  }

                  SValue.SRecord(
                    tyCon,
                    ImmArray.from(translatedCorrectFields.map(_._1)),
                    translatedCorrectFields.map(_._2).to(ArrayList),
                  )
                } else {

                  // note that we check the number of fields _before_ checking if we can do
                  // field reordering by looking at the labels. this means that it's forbidden to
                  // repeat keys even if we provide all the labels, which might be surprising
                  // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
                  // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
                  if (targetFieldsAndTypes.length != sourceElements.length) {
                    typeError(
                      s"Expecting ${targetFieldsAndTypes.length} field for record $tyCon, but got ${sourceElements.length}"
                    )
                  }

                  val fields =
                    labeledRecordToMap(sourceElements).fold(typeError, identity _) match {
                      case Some(labeledRecords) =>
                        targetFieldsAndTypes.map { case (lbl, typ) =>
                          labeledRecords
                            .get(lbl)
                            .fold(typeError(s"Missing record field '$lbl' for record $tyCon")) {
                              v =>
                                val replacedTyp = AstUtil.substitute(typ, subst)
                                lbl -> go(replacedTyp, v, newNesting)
                            }
                        }
                      case _ =>
                        (targetFieldsAndTypes zip sourceElements).map {
                          case ((targetField, typ), (mbLbl, v)) =>
                            mbLbl.foreach(sourceField =>
                              if (sourceField != targetField)
                                typeError(
                                  s"Mismatching record field label '$sourceField' (expecting '$targetField') for record $tyCon"
                                )
                            )
                            val replacedTyp = AstUtil.substitute(typ, subst)
                            targetField -> go(replacedTyp, v, newNesting)
                        }
                    }
                  SValue.SRecord(
                    tyCon,
                    fields.map(_._1),
                    fields.iterator.map(_._2).to(ArrayList),
                  )
                }
              case ValueEnum(mbId, constructor) if tyArgs.isEmpty =>
                checkUserTypeId(tyCon, mbId)
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
  // TODO: https://github.com/digital-asset/daml/issues/17082
  //  This is used by script and trigger, this should problaby use ValueTranslator.Config.Strict
  def strictTranslateValue(
      ty: Type,
      value: Value,
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(
      unsafeTranslateValue(ty, value, Config.Strict)
    )

  def translateValue(
      ty: Type,
      value: Value,
      config: Config,
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(
      unsafeTranslateValue(ty, value, config)
    )

}

object ValueTranslator {

  case class Config(
      ignorePackageId: Boolean,
      enableUpgrade: Boolean,
  )
  object Config {
    val Strict =
      Config(ignorePackageId = false, enableUpgrade = false)

    val Coerceable =
      Config(ignorePackageId = true, enableUpgrade = false)

    val Upgradeable =
      Config(ignorePackageId = true, enableUpgrade = true)
  }

}
