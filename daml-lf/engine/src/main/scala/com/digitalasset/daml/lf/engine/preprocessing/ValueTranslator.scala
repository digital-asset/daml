// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
package preprocessing

import com.daml.lf.CompiledPackages
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

import scala.annotation.tailrec

private[engine] final class ValueTranslator(compiledPackages: CompiledPackages) {

  import scala.Ordering.Implicits.infixOrderingOps

  import Preprocessor._

  // note: all the types in params must be closed.
  //
  // this is not tail recursive, but it doesn't really matter, since types are bounded
  // by what's in the source, which should be short enough...
  @throws[PreprocessorException]
  private[this] def replaceParameters(params: Iterable[(TypeVarName, Type)], typ0: Type): Type =
    if (params.isEmpty) { // optimization
      typ0
    } else {
      val paramsMap: Map[TypeVarName, Type] = params.toMap

      def go(typ: Type): Type =
        typ match {
          case TVar(v) =>
            paramsMap.getOrElse(
              v,
              fail(s"Got out of bounds type variable $v when replacing parameters"),
            )
          case TTyCon(_) | TBuiltin(_) | TNat(_) => typ
          case TApp(tyfun, arg) => TApp(go(tyfun), go(arg))
          case forall: TForall =>
            fail(
              s"Unexpected forall when replacing parameters in command translation -- all types should be serializable, and foralls are not: $forall"
            )
          case struct: TStruct =>
            fail(
              s"Unexpected struct when replacing parameters in command translation -- all types should be serializable, and structs are not: $struct"
            )
          case syn: TSynApp =>
            fail(
              s"Unexpected type synonym application when replacing parameters in command translation -- all types should be serializable, and synonyms are not: $syn"
            )
        }

      go(typ0)
    }

  @throws[PreprocessorException]
  private def labeledRecordToMap(
      fields: ImmArray[(Option[String], Value[ContractId])]
  ): Option[Map[String, Value[ContractId]]] = {
    @tailrec
    def go(
        fields: ImmArray[(Option[String], Value[ContractId])],
        map: Map[String, Value[ContractId]],
    ): Option[Map[String, Value[ContractId]]] = {
      fields match {
        case ImmArray() => Some(map)
        case ImmArrayCons((None, _), _) => None
        case ImmArrayCons((Some(label), value), tail) =>
          go(tail, map + (label -> value))
      }
    }

    go(fields, Map.empty)
  }

  @throws[PreprocessorException]
  private def unsafeGetPackage(pkgId: Ref.PackageId) =
    compiledPackages.getSignature(pkgId).getOrElse(throw PreprocessorMissingPackage(pkgId))

  @tailrec
  private[this] def destructApp(typ: Type, tyArgs: List[Type] = List.empty): (Type, List[Type]) =
    typ match {
      case TApp(tyFun, tyArg) => destructApp(tyFun, tyArg :: tyArgs)
      case otherwise => (otherwise, tyArgs)
    }

  // For efficient reason we do not produce here the monad Result[SValue] but rather throw
  // exception in case of error or package missing.
  @throws[PreprocessorException]
  private[preprocessing] def unsafeTranslateValue(
      ty: Type,
      value: Value[ContractId],
  ): (SValue, Set[Value.ContractId]) = {

    val cids = Set.newBuilder[Value.ContractId]

    def go(ty0: Type, value: Value[ContractId], nesting: Int = 0): SValue =
      if (nesting > Value.MAXIMUM_NESTING) {
        fail(s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}")
      } else {
        val newNesting = nesting + 1
        def typeMismatch = fail(s"mismatching type: $ty and value: $value")
        val (ty1, tyArgs) = destructApp(ty0)
        ty1 match {
          case TBuiltin(bt) =>
            tyArgs match {
              case Nil =>
                (bt, value) match {
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
                    typeMismatch
                }
              case typeArg0 :: Nil =>
                (bt, value) match {
                  case (BTNumeric, ValueNumeric(d)) =>
                    typeArg0 match {
                      case TNat(s) =>
                        Numeric.fromBigDecimal(s, d).fold(fail, SValue.SNumeric)
                      case _ =>
                        typeMismatch
                    }
                  case (BTContractId, ValueContractId(c)) =>
                    cids += c
                    SValue.SContractId(c)
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
                      SValue.SGenMap(
                        isTextMap = true,
                        entries = entries.iterator.map { case (k, v) =>
                          SValue.SText(k) -> go(typeArg0, v, newNesting)
                        },
                      )
                    }
                  case _ =>
                    typeMismatch
                }
              case typeArg0 :: typeArg1 :: Nil =>
                (bt, value) match {
                  case (BTGenMap, ValueGenMap(entries)) =>
                    if (entries.isEmpty) {
                      SValue.SValue.EmptyGenMap
                    } else {
                      SValue.SGenMap(
                        isTextMap = false,
                        entries = entries.iterator.map { case (k, v) =>
                          go(typeArg0, k, newNesting) -> go(typeArg1, v, newNesting)
                        },
                      )
                    }
                  case _ =>
                    typeMismatch
                }
              case _ =>
                typeMismatch
            }
          case TTyCon(tyCon) =>
            val pkg = unsafeGetPackage(tyCon.packageId)
            value match {
              // variant
              case ValueVariant(mbId, constructorName, val0) =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    fail(
                      s"Mismatching variant id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val (dataTypParams, variantDef) = assertRight(
                  pkg.lookupVariant(tyCon.qualifiedName)
                )
                variantDef.constructorRank.get(constructorName) match {
                  case None =>
                    fail(
                      s"Couldn't find provided variant constructor $constructorName in variant $tyCon"
                    )
                  case Some(rank) =>
                    val (_, argTyp) = variantDef.variants(rank)
                    val replacedTyp =
                      replaceParameters(dataTypParams.toSeq.view.map(_._1).zip(tyArgs), argTyp)
                    SValue.SVariant(
                      tyCon,
                      constructorName,
                      rank,
                      go(replacedTyp, val0, newNesting),
                    )
                }
              case ValueRecord(mbId, flds) =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    fail(
                      s"Mismatching record id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val (dataTypParams, DataRecord(recordFlds)) =
                  assertRight(pkg.lookupRecord(tyCon.qualifiedName))
                // note that we check the number of fields _before_ checking if we can do
                // field reordering by looking at the labels. this means that it's forbidden to
                // repeat keys even if we provide all the labels, which might be surprising
                // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
                // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
                if (recordFlds.length != flds.length) {
                  fail(
                    s"Expecting ${recordFlds.length} field for record $tyCon, but got ${flds.length}"
                  )
                }
                val params = dataTypParams.toSeq.view.map(_._1).zip(tyArgs)
                val fields = labeledRecordToMap(flds) match {
                  case None =>
                    (recordFlds zip flds).map { case ((lbl, typ), (mbLbl, v)) =>
                      mbLbl.foreach(lbl_ =>
                        if (lbl_ != lbl)
                          fail(s"Mismatching record label $lbl_ (expecting $lbl) for record $tyCon")
                      )
                      val replacedTyp = replaceParameters(params, typ)
                      lbl -> go(replacedTyp, v, newNesting)
                    }
                  case Some(labeledRecords) =>
                    recordFlds.map { case (lbl, typ) =>
                      labeledRecords
                        .get(lbl)
                        .fold(fail(s"Missing record label $lbl for record $tyCon")) { v =>
                          val replacedTyp = replaceParameters(params, typ)
                          lbl -> go(replacedTyp, v, newNesting)
                        }
                    }
                }
                def fieldNames = fields.map(_._1)
                val fieldValues = ArrayList(fields.map(_._2).toSeq: _*)
                if (
                  compiledPackages.packageLanguageVersion(tyCon.packageId) < LanguageVersion.v1_dev
                ) {
                  SValue.SRecord10(
                    tyCon,
                    fieldNames,
                    fieldValues,
                  )
                } else {
                  SValue.SRecord12(fieldValues)
                }

              case ValueEnum(mbId, constructor) if tyArgs.isEmpty =>
                mbId.foreach(id =>
                  if (id != tyCon)
                    fail(
                      s"Mismatching enum id, the type tells us $tyCon, but the value tells us $id"
                    )
                )
                val dataDef = assertRight(pkg.lookupEnum(tyCon.qualifiedName))
                dataDef.constructorRank.get(constructor) match {
                  case Some(rank) =>
                    SValue.SEnum(tyCon, constructor, rank)
                  case None =>
                    fail(s"Couldn't find provided variant constructor $constructor in enum $tyCon")
                }
              case _ =>
                typeMismatch
            }
          case _ =>
            typeMismatch
        }
      }

    go(ty, value) -> cids.result()
  }

  // This does not try to pull missing packages, return an error instead.
  def translateValue(ty: Type, value: Value[ContractId]): Either[Error, SValue] =
    safelyRun(unsafeTranslateValue(ty, value)._1)

}
