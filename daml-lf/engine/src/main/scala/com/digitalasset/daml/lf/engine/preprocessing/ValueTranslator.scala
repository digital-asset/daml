// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.data.Ref._
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
      fields: ImmArray[(Option[Name], Value)]
  ): Either[String, Option[Map[Name, Value]]] = {
    @tailrec
    def go(
        fields: FrontStack[(Option[Name], Value)],
        map: Map[Name, Value],
    ): Either[String, Option[Map[Name, Value]]] = {
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

  def preUpgradesTranslateRecord(
      r: ValueRecord,
      tyCon: TypeConName,
      tyArgs: List[Type],
      typeError: String => Nothing,
      rec: (Type, Value) => SValue,
  ): SValue.SRecord = {
    val flds = r.fields
    r.tycon.foreach(id =>
      if (id != tyCon)
        typeError(
          s"Mismatching record id, the type tells us $tyCon, but the value tells us $id"
        )
    )
    val lookupResult = handleLookup(pkgInterface.lookupDataRecord(tyCon))
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
    val oLabeledFlds = labeledRecordToMap(flds).fold(typeError, identity _)
    val fields = oLabeledFlds match {
      case None =>
        (recordFlds zip flds).map { case ((lbl, typ), (mbLbl, v)) =>
          mbLbl.foreach(lbl_ =>
            if (lbl_ != lbl)
              typeError(
                s"Mismatching record field label '$lbl_' (expecting '$lbl') for record $tyCon"
              )
          )
          val replacedTyp = AstUtil.substitute(typ, subst)
          lbl -> rec(replacedTyp, v)
        }
      case Some(labeledRecords) =>
        recordFlds.map { case (lbl, typ) =>
          labeledRecords
            .get(lbl)
            .fold(typeError(s"Missing record field '$lbl' for record $tyCon")) { v =>
              val replacedTyp = AstUtil.substitute(typ, subst)
              lbl -> rec(replacedTyp, v)
            }
        }
    }
    SValue.SRecord(
      tyCon,
      fields.map(_._1),
      fields.iterator.map(_._2).to(ArrayList),
    )
  }

  def postUpgradesTranslateRecord(
      r: ValueRecord,
      tyCon: TypeConName,
      tyArgs: List[Type],
      typeError: String => Nothing,
      rec: (Type, Value) => SValue,
  ): SValue.SRecord = {
    // TODO: Do we need to verify the typeConName matches (without packageid)?
    val lookupResult = handleLookup(pkgInterface.lookupDataRecord(tyCon))
    val recordFlds = lookupResult.dataRecord.fields

    // TODO: consider using mutable arrays

    val subst = lookupResult.subst(tyArgs)

    val oLabeledFlds = labeledRecordToMap(r.fields).fold(typeError, identity _)

    // correctFields: (correct only by label/position) give the value and type
    // incorrectFields: Left = missing fields, type given. Right = additional fields, value given.
    // In the case of labelled fields, we can have extra and missing fields at the same time
    //   but we disallow this. Quoting that the presence of extra fields implies an upgraded contract, and as such
    //   there cannot be missing fields - error
    val (correctFields, incorrectFields): (
        ImmArray[(Name, Value, Type)],
        Option[Either[ImmArray[(Name, Type)], ImmArray[(Option[Name], Value)]]],
    ) =
      oLabeledFlds match {
        // Not fully labelled, so order dependent
        // Additional fields should downgrade, missing fields should upgrade
        case None => {
          val correctFields = (recordFlds zip r.fields).map { case ((lbl, typ), (mbLbl, v)) =>
            mbLbl.foreach(lbl_ =>
              if (lbl_ != lbl)
                typeError(
                  s"Mismatching record field label '$lbl_' (expecting '$lbl') for record $tyCon"
                )
            )
            (lbl, v, typ)
          }
          val numS = r.fields.length
          val numT = recordFlds.length
          val incorrectFields =
            if (numT > numS) // Missing fields
              Some(Left(recordFlds.strictSlice(numS, numT)))
            else if (numS > numT) // Extra fields
              Some(Right(r.fields.strictSlice(numT, numS)))
            else
              None
          (correctFields, incorrectFields)
        }
        // Fully labelled, so not order dependent
        case Some(labeledFlds) => {
          // State is (correctFields (backwards), remainingSourceFieldsMap, missingFields (backwards))
          val initialState: (Seq[(Name, Value, Type)], Map[Name, Value], Seq[(Name, Type)]) =
            (Seq(), labeledFlds, Seq())

          val (backwardsCorrectFlds, remainingLabeledFlds, backwardsMissingFlds) =
            recordFlds.foldLeft(initialState) {
              // Taking known labels from the type, order retained but backwards, since we need to carry information forwards
              case ((fs, labeledFlds, Seq()), (lbl, ty)) =>
                labeledFlds.get(lbl).fold((fs, labeledFlds, Seq(lbl -> ty))) { fld =>
                  ((lbl, fld, ty) +: fs, labeledFlds - lbl, Seq())
                }
              // Taking unknown labels from the type, note that all unknown labels must be together at the end, so finding
              // a known label at this point means the record is malformed.
              case ((fs, labeledFlds, missingFlds), (lbl, ty)) =>
                labeledFlds
                  .get(lbl)
                  .fold(
                    (fs, labeledFlds, (lbl -> ty) +: missingFlds)
                  ) { _ =>
                    typeError(
                      s"Missing non-upgradeable fields (due to existence of $lbl): ${missingFlds.map(_._1).mkString(",")}"
                    )
                  }
            }

          val correctFields = ImmArray.from(backwardsCorrectFlds.reverse)
          val missingFields = ImmArray.from(backwardsMissingFlds.reverse)
          val extraFields = ImmArray.from(remainingLabeledFlds.toSeq.map { case (lbl, v) =>
            (Some(lbl), v)
          })

          val incorrectFields =
            if (extraFields.nonEmpty)
              if (missingFields.nonEmpty)
                typeError(
                  s"""Both additional and missing fields provided, upgrade/downgrade cannot be performed.
                     |Additional fields: ${remainingLabeledFlds.keys.mkString(",")}
                     |Missing fields: ${missingFields.map(_._1).iterator.mkString(",")}
                     """.stripMargin
                )
              else
                Some(Right(extraFields))
            else if (missingFields.nonEmpty)
              Some(Left(missingFields))
            else
              None

          (correctFields, incorrectFields)
        }
      }

    val translatedCorrectFields = correctFields.map { case (lbl, v, typ) =>
      val replacedTyp = AstUtil.substitute(typ, subst)
      lbl -> rec(replacedTyp, v)
    }

    val translatedFields = incorrectFields match {
      case None => translatedCorrectFields
      case Some(Left(missingFields)) =>
        translatedCorrectFields slowAppend missingFields.map {
          case (lbl, TApp(TBuiltin(BTOptional), _)) => lbl -> SValue.SOptional(None)
          case (lbl, _) =>
            typeError(
              s"Missing non-optional field $lbl, cannot upgrade non-optional fields."
            )
        }
      case Some(Right(extraFields)) => {
        extraFields.foreach {
          case (_, ValueOptional(None)) =>
          case (lbl, ValueOptional(Some(_))) =>
            typeError(
              s"An optional contract field ($lbl) with a value of Some may not be dropped during downgrading."
            )
          case (lbl, _) =>
            typeError(
              s"Found non-optional extra field $lbl, cannot remove for downgrading."
            )
        }
        translatedCorrectFields
      }
    }

    SValue.SRecord(
      tyCon,
      translatedFields.map(_._1),
      translatedFields.iterator.map(_._2).to(ArrayList),
    )
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
              case r: ValueRecord =>
                if (allowCompatibilityTransformations)
                  postUpgradesTranslateRecord(r, tyCon, tyArgs, typeError, go(_, _, newNesting))
                else
                  preUpgradesTranslateRecord(r, tyCon, tyArgs, typeError, go(_, _, newNesting))
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
