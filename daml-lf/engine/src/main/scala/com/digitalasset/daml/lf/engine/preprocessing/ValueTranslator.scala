// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
package preprocessing

import com.daml.lf.CompiledPackages
import com.daml.lf.data.{Ref, _}
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

private[engine] final class ValueTranslator(compiledPackages: CompiledPackages) {

  import Preprocessor._

  // note: all the types in params must be closed.
  //
  // this is not tail recursive, but it doesn't really matter, since types are bounded
  // by what's in the source, which should be short enough...
  @throws[PreprocessorException]
  private def replaceParameters(params: ImmArray[(TypeVarName, Type)], typ0: Type): Type =
    if (params.isEmpty) { // optimization
      typ0
    } else {
      val paramsMap: Map[TypeVarName, Type] = Map(params.toSeq: _*)

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
              s"Unexpected forall when replacing parameters in command translation -- all types should be serializable, and foralls are not: $forall")
          case struct: TStruct =>
            fail(
              s"Unexpected struct when replacing parameters in command translation -- all types should be serializable, and structs are not: $struct")
          case syn: TSynApp =>
            fail(
              s"Unexpected type synonym application when replacing parameters in command translation -- all types should be serializable, and synonyms are not: $syn")
        }

      go(typ0)
    }

  @throws[PreprocessorException]
  private def labeledRecordToMap(fields: ImmArray[(Option[String], Value[AbsoluteContractId])])
    : Option[Map[String, Value[AbsoluteContractId]]] = {
    @tailrec
    def go(
        fields: ImmArray[(Option[String], Value[AbsoluteContractId])],
        map: Map[String, Value[AbsoluteContractId]])
      : Option[Map[String, Value[AbsoluteContractId]]] = {
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
    compiledPackages.getPackage(pkgId).getOrElse(throw PreprocessorMissingPackage(pkgId))

  // For efficient reason we do not produce here the monad Result[SValue] but rather throw
  // exception in case of error or package missing.
  @throws[PreprocessorException]
  private[preprocessing] def unsafeTranslateValue(
      ty: Type,
      value: Value[AbsoluteContractId],
  ): (SValue, Set[Value.AbsoluteContractId]) = {

    val cids = Set.newBuilder[Value.AbsoluteContractId]

    def go(ty: Type, value: Value[AbsoluteContractId], nesting: Int = 0): SValue =
      if (nesting > Value.MAXIMUM_NESTING) {
        fail(s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}")
      } else {
        val newNesting = nesting + 1
        (ty, value) match {
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
            SValue.SOptional(
              mb.map((value: Value[AbsoluteContractId]) => go(elemType, value, newNesting)))

          // list
          case (TList(elemType), ValueList(ls)) =>
            SValue.SList(
              ls.map((value: Value[AbsoluteContractId]) => go(elemType, value, newNesting)))

          // textMap
          case (TTextMap(elemType), ValueTextMap(map)) =>
            type O[_] = HashMap[String, SValue]
            SValue.STextMap(
              map.iterator
                .map { case (k, v) => k -> go(elemType, v, newNesting) }
                .to[O])

          // genMap
          case (TGenMap(keyType, valueType), ValueGenMap(entries)) =>
            SValue.SGenMap(entries.iterator.map {
              case (k, v) =>
                go(keyType, k, newNesting) ->
                  go(valueType, v, newNesting)
            })

          // variants
          case (TTyConApp(typeVariantId, tyConArgs), ValueVariant(mbId, constructorName, val0)) =>
            mbId.foreach(id =>
              if (id != typeVariantId)
                fail(
                  s"Mismatching variant id, the type tells us $typeVariantId, but the value tells us $id"))
            val pkg = unsafeGetPackage(typeVariantId.packageId)
            val (dataTypParams, variantDef) =
              assertRight(PackageLookup.lookupVariant(pkg, typeVariantId.qualifiedName))
            variantDef.constructorRank.get(constructorName) match {
              case None =>
                fail(
                  s"Couldn't find provided variant constructor $constructorName in variant $typeVariantId")
              case Some(rank) =>
                val (_, argTyp) = variantDef.variants(rank)
                val instantiatedArgTyp =
                  replaceParameters(dataTypParams.map(_._1).zip(tyConArgs), argTyp)
                SValue.SVariant(
                  typeVariantId,
                  constructorName,
                  rank,
                  go(instantiatedArgTyp, val0, newNesting))
            }
          // records
          case (TTyConApp(typeRecordId, tyConArgs), ValueRecord(mbId, flds)) =>
            mbId.foreach(id =>
              if (id != typeRecordId)
                fail(
                  s"Mismatching record id, the type tells us $typeRecordId, but the value tells us $id"))
            val pkg = unsafeGetPackage(typeRecordId.packageId)
            val (dataTypParams, DataRecord(recordFlds, _)) =
              assertRight(PackageLookup.lookupRecord(pkg, typeRecordId.qualifiedName))
            // note that we check the number of fields _before_ checking if we can do
            // field reordering by looking at the labels. this means that it's forbidden to
            // repeat keys even if we provide all the labels, which might be surprising
            // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
            // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
            if (recordFlds.length != flds.length) {
              fail(
                s"Expecting ${recordFlds.length} field for record $typeRecordId, but got ${flds.length}")
            }
            val params = dataTypParams.map(_._1).zip(tyConArgs)
            val fields = labeledRecordToMap(flds) match {
              case None =>
                (recordFlds zip flds).map {
                  case ((lbl, typ), (mbLbl, v)) =>
                    mbLbl.foreach(lbl_ =>
                      if (lbl_ != lbl)
                        fail(
                          s"Mismatching record label $lbl_ (expecting $lbl) for record $typeRecordId"))
                    val replacedTyp = replaceParameters(params, typ)
                    lbl -> go(replacedTyp, v, newNesting)
                }
              case Some(labeledRecords) =>
                recordFlds.map {
                  case ((lbl, typ)) =>
                    labeledRecords
                      .get(lbl)
                      .fold(fail(s"Missing record label $lbl for record $typeRecordId")) { v =>
                        val replacedTyp = replaceParameters(params, typ)
                        lbl -> go(replacedTyp, v, newNesting)
                      }
                }
            }

            SValue.SRecord(
              typeRecordId,
              Ref.Name.Array(fields.map(_._1).toSeq: _*),
              ArrayList(fields.map(_._2).toSeq: _*)
            )

          case (TTyCon(typeEnumId), ValueEnum(mbId, constructor)) =>
            mbId.foreach(id =>
              if (id != typeEnumId)
                fail(
                  s"Mismatching enum id, the type tells us $typeEnumId, but the value tells us $id"))
            val pkg = unsafeGetPackage(typeEnumId.packageId)
            val dataDef = assertRight(PackageLookup.lookupEnum(pkg, typeEnumId.qualifiedName))
            dataDef.constructorRank.get(constructor) match {
              case Some(rank) =>
                SValue.SEnum(typeEnumId, constructor, rank)
              case None =>
                fail(s"Couldn't find provided variant constructor $constructor in enum $typeEnumId")
            }

          // every other pairs of types and values are invalid
          case (otherType, otherValue) =>
            fail(s"mismatching type: $otherType and value: $otherValue")
        }
      }

    go(ty, value) -> cids.result()
  }

  // This does not try to pull missing packages, return an error instead.
  def translateValue(ty: Type, value: Value[AbsoluteContractId]): Either[Error, SValue] =
    safelyRun(unsafeTranslateValue(ty, value)._1)

}
