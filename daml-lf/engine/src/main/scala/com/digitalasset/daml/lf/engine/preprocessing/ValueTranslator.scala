// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
package preprocessing

import com.daml.lf.CompiledPackages
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{Util => AstUtil}
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

import scala.annotation.tailrec

private[engine] final class ValueTranslator(compiledPackages: CompiledPackages) {

  import Preprocessor._

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
        val (ty1, tyArgs) = AstUtil.destructApp(ty0)
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
                      AstUtil.substitute(argTyp, dataTypParams.toSeq.view.map(_._1).zip(tyArgs))
                    SValue.SVariant(
                      tyCon,
                      constructorName,
                      rank,
                      go(replacedTyp, val0, newNesting),
                    )
                }
              // records
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
                val subst = dataTypParams.toSeq.view.map(_._1).zip(tyArgs).toMap
                val fields = labeledRecordToMap(flds) match {
                  case None =>
                    (recordFlds zip flds).map { case ((lbl, typ), (mbLbl, v)) =>
                      mbLbl.foreach(lbl_ =>
                        if (lbl_ != lbl)
                          fail(s"Mismatching record label $lbl_ (expecting $lbl) for record $tyCon")
                      )
                      val replacedTyp = AstUtil.substitute(typ, subst)
                      lbl -> go(replacedTyp, v, newNesting)
                    }
                  case Some(labeledRecords) =>
                    recordFlds.map { case (lbl, typ) =>
                      labeledRecords
                        .get(lbl)
                        .fold(fail(s"Missing record label $lbl for record $tyCon")) { v =>
                          val replacedTyp = AstUtil.substitute(typ, subst)
                          lbl -> go(replacedTyp, v, newNesting)
                        }
                    }
                }
                SValue.SRecord(
                  tyCon,
                  fields.map(_._1),
                  ArrayList(fields.map(_._2).toSeq: _*),
                )

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
