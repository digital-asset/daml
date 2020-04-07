// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import java.util

import com.daml.lf.CompiledPackages
import com.daml.lf.data.Ref
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.util.control.NoStackTrace

private[engine] object ValueTranslator {

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  // we use the following exceptions for easier error handling in translateValues

  private final case class ValueTranslationError(err: Error)
      extends RuntimeException(err.toString, null, true, false)

  private final case object ValueTranslationMissingPackage
      extends RuntimeException
      with NoStackTrace

  private def fail(s: String): Nothing =
    throw ValueTranslationError(Error(s))

  private def fail(e: Error): Nothing =
    throw ValueTranslationError(e)

  private def assertRight[X](either: Either[Error, X]): X = either match {
    case Left(e) => fail(e)
    case Right(v) => v
  }

}

private[engine] final class ValueTranslator(compiledPackages: CompiledPackages) {

  import ValueTranslator._

  // note: all the types in params must be closed.
  //
  // this is not tail recursive, but it doesn't really matter, since types are bounded
  // by what's in the source, which should be short enough...
  private[this] def replaceParameters(params: ImmArray[(TypeVarName, Type)], typ0: Type): Type =
    if (params.isEmpty) { // optimization
      typ0
    } else {
      val paramsMap: Map[TypeVarName, Type] = Map(params.toSeq: _*)

      def go(typ: Type): Type =
        typ match {
          case TVar(v) =>
            paramsMap.get(v) match {
              case None =>
                fail(s"Got out of bounds type variable $v when replacing parameters")
              case Some(ty) => ty
            }
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

  private[this] def labeledRecordToMap(
      fields: ImmArray[(Option[String], Value[AbsoluteContractId])])
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

  // This pulls all the dependencies of the types in `toProcess0`.
  private def ensurePackagesAreLoaded(
      toProcess0: List[Type],
      knownTyCons0: Set[Ref.TypeConName] = Set.empty
  ): Result[Unit] = {

    @tailrec
    def go(toProcess: List[Type], knownTyCons: Set[Ref.TypeConName]): Result[Unit] =
      toProcess match {
        case Nil =>
          ResultDone(())
        case typ :: rest =>
          typ match {
            case TApp(fun, arg) =>
              go(fun :: arg :: rest, knownTyCons)
            case TTyCon(tyCon @ Ref.Identifier(packageId, qualifiedName)) if !knownTyCons(tyCon) =>
              compiledPackages.packages.lift(packageId) match {
                case Some(pkg) =>
                  PackageLookup.lookupDataType(pkg, qualifiedName) match {
                    case Right(DDataType(_, _, dataType)) =>
                      val newRest = dataType match {
                        case DataRecord(fields, _) =>
                          fields.foldRight(rest)(_._2 :: _)
                        case DataVariant(variants) =>
                          variants.foldRight(rest)(_._2 :: _)
                        case DataEnum(_) =>
                          rest
                      }
                      go(newRest, knownTyCons + tyCon)
                    case Left(e) =>
                      ResultError(e)
                  }
                case None =>
                  ResultNeedPackage(packageId, _ => ensurePackagesAreLoaded(rest, knownTyCons))
              }
            case TTyCon(_) | TNat(_) | TBuiltin(_) =>
              go(rest, knownTyCons)
            case TVar(_) | TSynApp(_, _) | TForall(_, _) | TStruct(_) =>
              fail(s"unserializable type ${typ.pretty}")
          }
      }

    go(toProcess0, knownTyCons0)
  }

  private def unsafeGetPackage(pkgId: Ref.PackageId) =
    compiledPackages.getPackage(pkgId).getOrElse(throw ValueTranslationMissingPackage)

  // For efficient reason we do not produce here the monad Result[SValue] but rather throw
  // exception in case of error or package missing.
  private def unsafeTranslateValue(
      ty: Type,
      value: Value[AbsoluteContractId],
      nesting: Int = 0,
  ): SValue = {
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
        case (TContractId(typ), ValueContractId(c)) =>
          typ match {
            case TTyCon(_) => SValue.SContractId(c)
            case _ => fail(s"Expected a type constructor but found $typ.")
          }

        // optional
        case (TOptional(elemType), ValueOptional(mb)) =>
          SValue.SOptional(mb.map((value: Value[AbsoluteContractId]) =>
            unsafeTranslateValue(elemType, value, newNesting)))

        // list
        case (TList(elemType), ValueList(ls)) =>
          SValue.SList(ls.map((value: Value[AbsoluteContractId]) =>
            unsafeTranslateValue(elemType, value, newNesting)))

        // textMap
        case (TTextMap(elemType), ValueTextMap(map)) =>
          type O[_] = HashMap[String, SValue]
          SValue.STextMap(
            map.iterator
              .map { case (k, v) => k -> unsafeTranslateValue(elemType, v, newNesting) }
              .to[O])

        // genMap
        case (TGenMap(keyType, valueType), ValueGenMap(entries)) =>
          SValue.SGenMap(entries.iterator.map {
            case (k, v) =>
              unsafeTranslateValue(keyType, k, newNesting) -> unsafeTranslateValue(
                valueType,
                v,
                newNesting)
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
                unsafeTranslateValue(instantiatedArgTyp, val0, newNesting))
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
                  lbl -> unsafeTranslateValue(replacedTyp, v, newNesting)
              }
            case Some(labeledRecords) =>
              recordFlds.map {
                case ((lbl, typ)) =>
                  labeledRecords
                    .get(lbl)
                    .fold(fail(s"Missing record label $lbl for record $typeRecordId")) { v =>
                      val replacedTyp = replaceParameters(params, typ)
                      lbl -> unsafeTranslateValue(replacedTyp, v, newNesting)
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
  }

  /**
    * Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  private[engine] def translateValue(ty0: Type, v0: Value[AbsoluteContractId]): Result[SValue] = {
    def start: Result[SValue] =
      try {
        ResultDone(unsafeTranslateValue(ty0, v0))
      } catch {
        case ValueTranslationError(e) =>
          ResultError(e)
        case ValueTranslationMissingPackage =>
          // if one package is missing, we pull all of the missing packages recursively, and restart.
          ensurePackagesAreLoaded(List(ty0)).flatMap(_ => start)
      }

    start
  }

}
