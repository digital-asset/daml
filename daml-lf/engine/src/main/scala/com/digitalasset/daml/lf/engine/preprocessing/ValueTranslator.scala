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
import java.util

private[lf] final class ValueTranslator(
    pkgInterface: language.PackageInterface,
    requireV1ContractIdSuffix: Boolean,
) {

  import ValueTranslator._
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

                if (config.enableUpgrade) {
                  // We handle the (non-)upgrade cases for record separately
                  // In case of upgrade, we require the field are record.

                  // This code implements the compatibility transformation used for up/down-grading
                  // And handles the cases:
                  // - UPGRADE:   numT > numS : creates a None for each missing fields.
                  // - DOWNGRADE: numS > numT : drops each extra field, ensuring it is None.
                  //
                  // When numS == numT, we wont hit the code marked either as UPGRADE or DOWNGRADE,
                  // although it is still possible that the source and target types are different,
                  // but since we don't consult the source type (may be unavailable), we wont know.

                  val numS: Int = sourceElements.length
                  val numT: Int = targetFieldsAndTypes.length

                  // traverse the sourceElements, "get"ing the corresponding target type
                  // when there is no corresponding type, we must be downgrading, and so we insist the value is None
                  val values0: List[SValue] =
                    sourceElements.toSeq.view.zipWithIndex.flatMap { case ((optName, v), i) =>
                      targetFieldsAndTypes.get(i) match {
                        case Some((targetField, targetFieldType)) =>
                          optName match {
                            case None => ()
                            case Some(sourceField) =>
                              // value is not normalized; check field names match
                              if (sourceField != targetField)
                                typeError(
                                  s"Mismatching record field label '$sourceField' (expecting '$targetField') for record $tyCon"
                                )

                          }
                          val typ: Type = AstUtil.substitute(targetFieldType, subst)
                          val sv: SValue = go(typ, v, newNesting)
                          List(sv)
                        case None => // DOWNGRADE
                          // i ranges from 0 to numS-1. So i >= numT implies numS > numT
                          assert((numS > i) && (i >= numT))
                          v match {
                            case ValueOptional(None) =>
                              List() // ok, drop
                            case ValueOptional(Some(_)) =>
                              typeError(
                                "An optional contract field with a value of Some may not be dropped during downgrading."
                              )
                            case _ =>
                              typeError(
                                "Unexpected non-optional extra contract field encountered during downgrading."
                              )
                          }
                      }
                    }.toList

                  val fields: ImmArray[Ref.Name] =
                    targetFieldsAndTypes.map { case (name, _) =>
                      name
                    }

                  val values: util.ArrayList[SValue] = {
                    if (numT > numS) {

                      def isOptionalType(typ: Type): Boolean = {
                        typ match {
                          case TApp(TBuiltin(BTOptional), _) => true
                          case _ => false
                        }
                      }

                      val extraFieldsWithNonOptionType: List[Ref.Name] =
                        targetFieldsAndTypes.toList
                          .drop(numS)
                          .filter { case (_, typ) => !isOptionalType(typ) }
                          .map { case (name, _) => name }

                      if (extraFieldsWithNonOptionType.length == 0) {
                        values0.padTo(numT, SValue.SValue.None) // UPGRADE
                      } else {
                        typeError(
                          "Unexpected non-optional extra contract field encountered during downgrading."
                        )
                      }
                    } else {
                      values0
                    }
                  }.to(ArrayList)

                  SValue.SRecord(tyCon, fields, values)

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

                  val fields = labeledRecordToMap(sourceElements) match {
                    case Some(labeledRecords) if config.allowFieldReordering =>
                      targetFieldsAndTypes.map { case (lbl, typ) =>
                        labeledRecords
                          .get(lbl)
                          .fold(typeError(s"Missing record field '$lbl' for record $tyCon")) { v =>
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
  //  This is used by script, this should problaby use ValueTranslator.Config.Strict
  def strictTranslateValue(
      ty: Type,
      value: Value,
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(
      unsafeTranslateValue(ty, value, Config.Legacy)
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
      allowFieldReordering: Boolean,
      ignorePackageId: Boolean,
      enableUpgrade: Boolean,
  ) {
    if (enableUpgrade) {
      assert(!allowFieldReordering, "record fields reordering is possible only if upgrade is off")
    }
  }
  object Config {
    val Strict =
      Config(allowFieldReordering = false, ignorePackageId = false, enableUpgrade = false)

    // Lenient Legacy config, i.e. pre-upgrade
    val Legacy =
      Config(allowFieldReordering = true, ignorePackageId = false, enableUpgrade = false)

    val Upgradeable =
      Config(allowFieldReordering = false, ignorePackageId = true, enableUpgrade = true)
  }

}
