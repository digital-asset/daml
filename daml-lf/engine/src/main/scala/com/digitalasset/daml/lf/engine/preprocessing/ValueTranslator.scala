// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
package preprocessing

import com.daml.lf.CompiledPackages
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
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

    def go(
        ty: Type,
        value: Value[ContractId],
        subst0: DelayedTypeSubstitution = DelayedTypeSubstitution.Empty,
        nesting: Int = 0,
    ): SValue =
      if (nesting > Value.MAXIMUM_NESTING) {
        fail(s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}")
      } else {
        val (subst1, nonVariableType) = subst0.apply(ty)
        val newNesting = nesting + 1
        (nonVariableType, value) match {
          // simple values
          case (TUnit, ValueUnit) =>
            SValue.SUnit
          case (TBool, ValueBool(b)) =>
            if (b) SValue.SValue.True else SValue.SValue.False
          case (TInt64, ValueInt64(i)) =>
            SValue.SInt64(i)
          case (TTimestamp, ValueTimestamp(t)) =>
            SValue.STimestamp(t)
          case (TDate, ValueDate(t)) =>
            SValue.SDate(t)
          case (TText, ValueText(t)) =>
            SValue.SText(t)
          case (TNumeric(TNat(s)), ValueNumeric(d)) =>
            Numeric.fromBigDecimal(s, d).fold(fail, SValue.SNumeric)
          case (TParty, ValueParty(p)) =>
            SValue.SParty(p)
          case (TContractId(_), ValueContractId(c)) =>
            cids += c
            SValue.SContractId(c)

          // optional
          case (TOptional(elemType), ValueOptional(mb)) =>
            SValue.SOptional(mb.map(go(elemType, _, subst1, newNesting)))

          // list
          case (TList(elemType), ValueList(ls)) =>
            SValue.SList(ls.map(go(elemType, _, subst1, newNesting)))

          // textMap
          case (TTextMap(elemType), ValueTextMap(entries)) =>
            SValue.SGenMap(
              isTextMap = true,
              entries = entries.iterator.map { case (k, v) =>
                SValue.SText(k) -> go(elemType, v, subst1, newNesting)
              },
            )

          // genMap
          case (TGenMap(keyType, valueType), ValueGenMap(entries)) =>
            SValue.SGenMap(
              isTextMap = false,
              entries = entries.iterator.map { case (k, v) =>
                go(keyType, k, subst1, newNesting) -> go(valueType, v, subst1, newNesting)
              },
            )

          // variants
          case (TTyConApp(tycon, tyConArgs), ValueVariant(mbId, constructorName, val0)) =>
            mbId.foreach(id =>
              if (id != tycon)
                fail(
                  s"Mismatching variant id, the type tells us $tycon, but the value tells us $id"
                )
            )
            val pkg = unsafeGetPackage(tycon.packageId)
            val (dataTypParams, variantDef) =
              assertRight(SignatureLookup.lookupVariant(pkg, tycon.qualifiedName))
            variantDef.constructorRank.get(constructorName) match {
              case None =>
                fail(
                  s"Couldn't find provided variant constructor $constructorName in variant $tycon"
                )

              case Some(rank) =>
                val (_, argTyp) = variantDef.variants(rank)
                val newSubst =
                  subst1.introVars(dataTypParams.toSeq.view.map(_._1) zip tyConArgs.toSeq)
                SValue.SVariant(
                  tycon,
                  constructorName,
                  rank,
                  go(argTyp, val0, newSubst, newNesting),
                )
            }
          // records
          case (TTyConApp(tycon, tyConArgs), ValueRecord(mbId, flds)) =>
            mbId.foreach(id =>
              if (id != tycon)
                fail(s"Mismatching record id, the type tells us $tycon, but the value tells us $id")
            )
            val pkg = unsafeGetPackage(tycon.packageId)
            val (dataTypParams, DataRecord(recordFlds)) =
              assertRight(SignatureLookup.lookupRecord(pkg, tycon.qualifiedName))
            // note that we check the number of fields _before_ checking if we can do
            // field reordering by looking at the labels. this means that it's forbidden to
            // repeat keys even if we provide all the labels, which might be surprising
            // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
            // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
            if (recordFlds.length != flds.length) {
              fail(
                s"Expecting ${recordFlds.length} field for record $tycon, but got ${flds.length}"
              )
            }
            val newSubst = subst1.introVars(dataTypParams.toSeq.view.map(_._1) zip tyConArgs.toSeq)
            val fields = labeledRecordToMap(flds) match {
              case None =>
                (recordFlds zip flds).map { case ((lbl, typ), (mbLbl, v)) =>
                  mbLbl.foreach(lbl_ =>
                    if (lbl_ != lbl)
                      fail(
                        s"Mismatching record label $lbl_ (expecting $lbl) for record $tycon"
                      )
                  )

                  lbl -> go(typ, v, newSubst, newNesting)
                }
              case Some(labeledRecords) =>
                recordFlds.map { case (lbl, typ) =>
                  labeledRecords
                    .get(lbl)
                    .fold(fail(s"Missing record label $lbl for record $tycon")) { v =>
                      lbl -> go(typ, v, newSubst, newNesting)
                    }
                }
            }

            SValue.SRecord(
              tycon,
              fields.map(_._1),
              ArrayList(fields.map(_._2).toSeq: _*),
            )

          case (TTyCon(tycon), ValueEnum(mbId, constructor)) =>
            mbId.foreach(id =>
              if (id != tycon)
                fail(s"Mismatching enum id, the type tells us $tycon, but the value tells us $id")
            )
            val pkg = unsafeGetPackage(tycon.packageId)
            val dataDef = assertRight(SignatureLookup.lookupEnum(pkg, tycon.qualifiedName))
            dataDef.constructorRank.get(constructor) match {
              case Some(rank) =>
                SValue.SEnum(tycon, constructor, rank)
              case None =>
                fail(s"Couldn't find provided variant constructor $constructor in enum $tycon")
            }

          // every other pairs of types and values are invalid
          case (otherType, otherValue) =>
            fail(s"mismatching type: $otherType and value: $otherValue")
        }
      }

    go(ty, value) -> cids.result()
  }

  // This does not try to pull missing packages, return an error instead.
  def translateValue(ty: Type, value: Value[ContractId]): Either[Error, SValue] =
    safelyRun(unsafeTranslateValue(ty, value)._1)

}
