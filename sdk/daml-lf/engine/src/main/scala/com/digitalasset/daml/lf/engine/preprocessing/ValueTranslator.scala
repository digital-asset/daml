// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.TypeDestructor
import com.digitalasset.daml.lf.speedy.{ArrayList, SValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._

import scala.annotation.tailrec

private[lf] final class ValueTranslator(
    pkgInterface: language.PackageInterface,
    requireContractIdSuffix: Boolean,
    shouldCheckDataSerializable: Boolean = true,
) {

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
    if (requireContractIdSuffix) {
      case cid: ContractId.V1 =>
        if (cid.suffix.isEmpty)
          throw Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(cid)
      case cid: ContractId.V2 =>
        // We forbid only local contract IDs in Engine commands, but not relative contract IDs
        // because relative contract IDs may appear in reinterpretation of projections
        if (cid.suffix.isEmpty)
          throw Error.Preprocessing.IllegalContractId.NonSuffixV2ContractId(cid)
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
  ): SValue = {
    import TypeDestructor.SerializableTypeF._
    val Destructor = TypeDestructor(pkgInterface)

    // TODO: https://github.com/digital-asset/daml/issues/17082
    //   Should we consider factorizing this code with Seedy.Machine#importValues
    def go(ty0: Type, value0: Value, nesting: Int): SValue =
      if (nesting > Value.MAXIMUM_NESTING) {
        throw Error.Preprocessing.ValueNesting(value)
      } else {
        val newNesting = nesting + 1
        def typeError(msg: String = s"mismatching type: ${ty0.pretty} and value: $value0") =
          throw Error.Preprocessing.TypeMismatch(ty0, value0, msg)
        def destruct(typ: Type) =
          Destructor.destruct(typ, shouldCheckDataSerializable) match {
            case Right(value) => value
            case Left(TypeDestructor.Error.TypeError(err)) =>
              typeError(err)
            case Left(TypeDestructor.Error.LookupError(err)) =>
              throw Error.Preprocessing.Lookup(err)
          }

        def checkUserTypeId(targetType: Ref.TypeConId, mbSourceType: Option[Ref.TypeConId]) =
          mbSourceType.foreach(sourceType =>
            // In case of upgrade we simply ignore the package ID.
            if (targetType.qualifiedName != sourceType.qualifiedName)
              typeError(
                s"Mismatching variant id, the type tells us ${targetType.qualifiedName}, but the value tells us ${sourceType.qualifiedName}"
              )
          )

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
            SValue.SText(t)
          case (PartyF, ValueParty(p)) =>
            SValue.SParty(p)
          case (NumericF(s), ValueNumeric(d)) =>
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
              SValue.SMap(
                isTextMap = false,
                entries = entries.toSeq.view.map { case (k, v) =>
                  go(a, k, newNesting) -> go(b, v, newNesting)
                },
              )
            }
          case (TextMapF(a), ValueTextMap(entries)) =>
            if (entries.isEmpty) {
              SValue.SValue.EmptyTextMap
            } else {
              SValue.SMap(
                isTextMap = true,
                entries = entries.toImmArray.toSeq.view.map { case (k, v) =>
                  SValue.SText(k) -> go(a, v, newNesting)
                }.toList,
              )
            }
          case (
                vF @ VariantF(tyCon, _, _, consTyp),
                ValueVariant(mbId, constructorName, val0),
              ) =>
            checkUserTypeId(tyCon, mbId)
            val i = handleLookup(vF.consRank(constructorName))
            SValue.SVariant(
              tyCon,
              constructorName,
              i,
              go(consTyp(i), val0, newNesting),
            )
          // records
          case (
                RecordF(tyCon, _, fieldNames, filedTypes),
                ValueRecord(mbId, sourceElements),
              ) =>
            checkUserTypeId(tyCon, mbId)

            def addMissingField(lbl: Ref.Name, ty: Type): (Option[Ref.Name], Value) =
              destruct(ty) match {
                // If missing field is optional, fill it with None
                case OptionalF(_) => (Some(lbl), Value.ValueOptional(None))
                // Else, throw error
                case _ =>
                  typeError(
                    s"Missing non-optional field \"$lbl\", cannot upgrade non-optional fields."
                  )
              }

            val oLabeledFlds = labeledRecordToMap(sourceElements).fold(typeError, identity _)

            // correctFields: (correct only by label/position) gives the value and type, length == targetFieldsAndTypes
            //   filled with Nones when type is Optional
            // extraFields: Unknown additional fields with name and value
            val (correctFields, extraFields): (
                List[(Ref.Name, Value, Type)],
                List[(Option[Ref.Name], Value)],
            ) =
              oLabeledFlds match {
                // Not fully labelled (or reordering disabled), so order dependent
                // Additional fields should downgrade, missing fields should upgrade
                case None => {
                  val correctFields = (fieldNames.view zip filedTypes).zipWithIndex.map {
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
                  }.toList
                  val numS = sourceElements.length
                  val numT = filedTypes.length
                  // We have extra fields
                  val extraFields =
                    if (numS > numT)
                      sourceElements.strictSlice(numT, numS).toList
                    else
                      List.empty

                  (correctFields, extraFields)
                }
                // Fully labelled and allowed to re-order
                case Some(labeledFlds) =>
                  // new logic
                  // iterate the expected fields, replace any missing with none
                  val correctFields = (fieldNames.view zip filedTypes).map { case (lbl, ty) =>
                    (lbl, labeledFlds.getOrElse(lbl, addMissingField(lbl, ty)._2), ty)
                  }.toList
                  val extraFields = (labeledFlds -- fieldNames).view.map { case (lbl, v) =>
                    (Some(lbl), v)
                  }.toList
                  (correctFields, extraFields)
              }

            // Recursive substitution
            val translatedCorrectFields = correctFields.map { case (lbl, v, typ) =>
              lbl -> go(typ, v, newNesting)
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
          case (eF @ EnumF(tyCon, _, _), ValueEnum(mbId, constructor)) =>
            checkUserTypeId(tyCon, mbId)
            val rank = handleLookup(eF.consRank(constructor))
            SValue.SEnum(tyCon, constructor, rank)
          case _ =>
            typeError()
        }
      }

    go(ty, value, 0)
  }

  // This does not try to pull missing packages, return an error instead.
  // TODO: https://github.com/digital-asset/daml/issues/17082
  //  This is used by script, this should problaby use ValueTranslator.Config.Strict
  def strictTranslateValue(
      ty: Type,
      value: Value,
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(
      unsafeTranslateValue(ty, value)
    )

  def translateValue(
      ty: Type,
      value: Value,
  ): Either[Error.Preprocessing.Error, SValue] =
    safelyRun(
      unsafeTranslateValue(ty, value)
    )

}
