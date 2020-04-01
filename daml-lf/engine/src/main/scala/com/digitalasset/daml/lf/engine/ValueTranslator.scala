// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine

import java.util

import com.daml.lf.CompiledPackages
import com.daml.lf.data.Ref.Name
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue.SValueContainer
import com.daml.lf.value.Value
import com.daml.lf.value.Value._

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

private[engine] object ValueTranslator {

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  // we use this for easier error handling in translateValues
  private final case class ValueTranslationException(err: Error)
      extends RuntimeException(err.toString, null, true, false)

  private def fail(s: String): Nothing =
    throw ValueTranslationException(Error(s))

}

private[engine] class ValueTranslator(compiledPackages: CompiledPackages) {

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

  private object SValueResultDone extends SValueContainer[ResultDone[SValue]] {
    override def apply(value: SValue): ResultDone[SValue] = ResultDone(value)
  }

  // since we get these values from third-party users of the library, check the recursion limit
  // here, too.
  private[engine] def translateValue(ty0: Type, v0: Value[AbsoluteContractId]): Result[SValue] = {
    import SValue._
    import scalaz.std.option._
    import scalaz.syntax.traverse.ToTraverseOps

    def exceptionToResultError[A](x: => Result[A]): Result[A] =
      try {
        x
      } catch {
        case ValueTranslationException(err) => ResultError(err)
      }

    def go(nesting: Int, ty: Type, value: Value[AbsoluteContractId]): Result[SValue] = {
      // we use this to restart when we get a new package that allows us to make progress.
      def restart = exceptionToResultError(go(nesting, ty, value))

      if (nesting > Value.MAXIMUM_NESTING) {
        fail(s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}")
      } else {
        val newNesting = nesting + 1
        (ty, value) match {
          // simple values
          case (TUnit, ValueUnit) =>
            SValueResultDone.Unit
          case (TBool, ValueBool(b)) =>
            SValueResultDone.bool(b)
          case (TInt64, ValueInt64(i)) =>
            ResultDone(SInt64(i))
          case (TTimestamp, ValueTimestamp(t)) =>
            ResultDone(STimestamp(t))
          case (TDate, ValueDate(t)) =>
            ResultDone(SDate(t))
          case (TText, ValueText(t)) =>
            ResultDone(SText(t))
          case (TNumeric(TNat(s)), ValueNumeric(d)) =>
            Numeric.fromBigDecimal(s, d).fold(fail, d => ResultDone(SNumeric(d)))
          case (TParty, ValueParty(p)) =>
            ResultDone(SParty(p))
          case (TContractId(typ), ValueContractId(c)) =>
            typ match {
              case TTyCon(_) => ResultDone(SContractId(c))
              case _ => fail(s"Expected a type constructor but found $typ.")
            }

          // optional
          case (TOptional(elemType), ValueOptional(mb)) =>
            mb.traverseU(go(newNesting, elemType, _)).map(SOptional)

          // list
          case (TList(elemType), ValueList(ls)) =>
            ls.toImmArray.traverseU(go(newNesting, elemType, _)).map(es => SList(FrontStack(es)))

          // textMap
          case (TTextMap(elemType), ValueTextMap(map)) =>
            map.toImmArray
              .traverseU {
                case (key0, value0) => go(newNesting, elemType, value0).map(key0 -> _)
              }
              .map(l => STextMap(HashMap(l.toSeq: _*)))

          // genMap
          case (TGenMap(keyType, valueType), ValueGenMap(entries)) =>
            entries
              .traverseU {
                case (key0, value0) =>
                  for {
                    key <- go(newNesting, keyType, key0)
                    value <- go(newNesting, valueType, value0)
                  } yield key -> value
              }
              .map(l => SGenMap(l.iterator))

          // variants
          case (TTyConApp(tyCon, tyConArgs), ValueVariant(mbVariantId, constructorName, val0)) =>
            val variantId = tyCon
            mbVariantId match {
              case Some(variantId_) if variantId != variantId_ =>
                fail(
                  s"Mismatching variant id, the type tells us $variantId, but the value tells us $variantId_")
              case _ =>
                compiledPackages.getPackage(variantId.packageId) match {
                  // if the package is not there, look it up and restart. stack safe since this will be done
                  // very few times as the cache gets warm. this is also why we do not use the `Result.needDataType`, which
                  // would consume stack regardless
                  case None =>
                    Result.needPackage(variantId.packageId, _ => restart)
                  case Some(pkg) =>
                    PackageLookup.lookupVariant(pkg, variantId.qualifiedName) match {
                      case Left(err) => ResultError(err)
                      case Right((dataTypParams, variantDef: DataVariant)) =>
                        variantDef.constructorRank.get(constructorName) match {
                          case None =>
                            fail(
                              s"Couldn't find provided variant constructor $constructorName in variant $variantId")
                          case Some(rank) =>
                            val (_, argTyp) = variantDef.variants(rank)
                            if (dataTypParams.length != tyConArgs.length) {
                              sys.error(
                                "TODO(FM) impossible: type constructor applied to wrong number of parameters, this should never happen on a well-typed package, return better error")
                            }
                            val instantiatedArgTyp =
                              replaceParameters(dataTypParams.map(_._1).zip(tyConArgs), argTyp)
                            go(newNesting, instantiatedArgTyp, val0).map(
                              SVariant(tyCon, constructorName, rank, _))
                        }
                    }
                }
            }
          // records
          case (TTyConApp(tyCon, tyConArgs), ValueRecord(mbRecordId, flds)) =>
            val recordId = tyCon
            mbRecordId match {
              case Some(recordId_) if recordId != recordId_ =>
                fail(
                  s"Mismatching record id, the type tells us $recordId, but the value tells us $recordId_")
              case _ =>
                compiledPackages.getPackage(recordId.packageId) match {
                  // if the package is not there, look it up and restart. stack safe since this will be done
                  // very few times as the cache gets warm. this is also why we do not use the `Result.needDataType`, which
                  // would consume stack regardless
                  case None =>
                    Result.needPackage(recordId.packageId, _ => restart)
                  case Some(pkg) =>
                    PackageLookup.lookupRecord(pkg, recordId.qualifiedName) match {
                      case Left(err) => ResultError(err)
                      case Right((dataTypParams, DataRecord(recordFlds, _))) =>
                        // note that we check the number of fields _before_ checking if we can do
                        // field reordering by looking at the labels. this means that it's forbidden to
                        // repeat keys even if we provide all the labels, which might be surprising
                        // since in JavaScript / Scala / most languages (but _not_ JSON, interestingly)
                        // it's ok to do `{"a": 1, "a": 2}`, where the second occurrence would just win.
                        if (recordFlds.length != flds.length) {
                          fail(
                            s"Expecting ${recordFlds.length} field for record $recordId, but got ${flds.length}")
                        }
                        if (dataTypParams.length != tyConArgs.length) {
                          sys.error(
                            "TODO(FM) impossible: type constructor applied to wrong number of parameters, this should never happen on a well-typed package, return better error")
                        }
                        val params = dataTypParams.map(_._1).zip(tyConArgs)
                        labeledRecordToMap(flds)
                          .fold {
                            recordFlds.zip(flds).traverseU {
                              case ((lbl, typ), (mbLbl, v)) =>
                                mbLbl
                                  .filter(_ != lbl)
                                  .foreach(lbl_ =>
                                    fail(
                                      s"Mismatching record label $lbl_ (expecting $lbl) for record $recordId"))
                                val replacedTyp = replaceParameters(params, typ)
                                go(newNesting, replacedTyp, v).map(e => (lbl, e))
                            }
                          } { labeledRecords =>
                            recordFlds.traverseU {
                              case ((lbl, typ)) =>
                                labeledRecords
                                  .get(lbl)
                                  .fold(fail(s"Missing record label $lbl for record $recordId")) {
                                    v =>
                                      val replacedTyp = replaceParameters(params, typ)
                                      go(newNesting, replacedTyp, v).map(e => (lbl, e))
                                  }
                            }
                          }
                          .map(
                            flds =>
                              SRecord(
                                tyCon,
                                Name.Array(flds.map(_._1).toSeq: _*),
                                ArrayList(flds.map(_._2).toSeq: _*)
                            ))
                    }
                }
            }

          case (TTyCon(id), ValueEnum(mbId, constructor)) =>
            mbId match {
              case Some(id_) if id_ != id =>
                fail(s"Mismatching enum id, the type tells us $id, but the value tells us $id_")
              case _ =>
                compiledPackages.getPackage(id.packageId) match {
                  // if the package is not there, look it up and restart. stack safe since this will be done
                  // very few times as the cache gets warm. this is also why we do not use the `Result.needDataType`, which
                  // would consume stack regardless
                  case None =>
                    Result.needPackage(id.packageId, _ => restart)
                  case Some(pkg) =>
                    PackageLookup.lookupEnum(pkg, id.qualifiedName) match {
                      case Left(err) => ResultError(err)
                      case Right(dataDef: DataEnum) =>
                        dataDef.constructorRank.get(constructor) match {
                          case Some(rank) =>
                            ResultDone(SEnum(id, constructor, rank))
                          case None =>
                            fail(
                              s"Couldn't find provided variant constructor $constructor in enum $id")
                        }
                    }
                }
            }

          // every other pairs of types and values are invalid
          case (otherType, otherValue) =>
            fail(s"mismatching type: $otherType and value: $otherValue")
        }
      }
    }

    exceptionToResultError(go(0, ty0, v0))
  }

}
