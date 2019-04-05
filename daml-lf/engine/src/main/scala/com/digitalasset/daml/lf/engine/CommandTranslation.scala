// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Util._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._

private[engine] object CommandTranslation {
  def apply(compiledPackages: ConcurrentCompiledPackages): CommandTranslation = {
    new CommandTranslation(compiledPackages)
  }

  private def tEntry(elemType: Type) =
    TTuple(ImmArray(("key", TBuiltin(BTText)), ("value", elemType)))

  private def entry(key: String, value: Expr): Expr =
    ETupleCon(ImmArray("key" -> EPrimLit(PLText(key)), "value" -> value))

  private def emptyMap(elemType: Type) =
    EBuiltin(BMapEmpty) eTyApp elemType

  private def fold(aType: Type, bType: Type, f: Expr, b: Expr, as: Expr) =
    EBuiltin(BFoldl) eTyApp (aType, bType) eApp (f, b, as)

  private def insert(elemType: Type, key: Expr, value: Expr, map: Expr) =
    EBuiltin(BMapInsert) eTyApp elemType eApp (key, value, map)

  private def get(key: String, tuple: Expr) =
    ETupleProj(key, tuple)

  private def buildList(elemType: Type, list: ImmArray[Expr]) =
    ECons(elemType, list, ENil(elemType))

  private def buildMap(elemType: Type, list: Expr): Expr = {
    val f =
      EAbs(
        "acc" -> TMap(elemType),
        EAbs(
          "entry" -> tEntry(elemType),
          insert(elemType, get("key", EVar("entry")), get("value", EVar("entry")), EVar("acc")),
          None),
        None)
    fold(tEntry(elemType), TMap(elemType), f, emptyMap(elemType), list)
  }

}

private[engine] class CommandTranslation(compiledPackages: ConcurrentCompiledPackages) {
  import CommandTranslation._

  // we use this for easier error handling in translateValues
  private[this] case class CommandTranslationException(err: Error)
      extends RuntimeException(err.toString, null, true, false)

  private[this] def fail[A](s: String): A =
    throw CommandTranslationException(Error(s))

  // note: all the types in params must be closed.
  //
  // this is not tail recursive, but it doesn't really matter, since types are bounded
  // by what's in the source, which should be short enough...
  private[this] def replaceParameters(params: ImmArray[(String, Type)], typ0: Type): Type =
    if (params.isEmpty) { // optimization
      typ0
    } else {
      val paramsMap: Map[String, Type] = Map(params.toSeq: _*)
      def go(typ: Type): Type =
        typ match {
          case TVar(v) =>
            paramsMap.get(v) match {
              case None =>
                fail(s"Got out of bounds type variable $v when replacing parameters")
              case Some(ty) => ty
            }
          case tycon: TTyCon => tycon
          case bltin: TBuiltin => bltin
          case TApp(tyfun, arg) => TApp(go(tyfun), go(arg))
          case forall: TForall =>
            fail(
              s"Unexpected forall when replacing parameters in command translation -- all types should be serialiable, and foralls are not: $forall")
          case tuple: TTuple =>
            fail(
              s"Unexpected tuple when replacing parameters in command translation -- all types should be serialiable, and tuples are not: $tuple")
        }
      go(typ0)
    }

  // since we get these values from third-party users of the library, check the recursion limit
  // here, too.
  private[engine] def translateValue(
      ty0: Type,
      v0: VersionedValue[AbsoluteContractId]): Result[Expr] = {
    import scalaz.syntax.traverse.ToTraverseOps

    def exceptionToResultError[A](x: => Result[A]): Result[A] =
      try {
        x
      } catch {
        case CommandTranslationException(err) => ResultError(err)
      }

    def go(nesting: Int, ty: Type, value: Value[AbsoluteContractId]): Result[Expr] = {
      // we use this to restart when we get a new package that allows us to make progress.
      def restart = exceptionToResultError(go(nesting, ty, value))

      if (nesting > Value.MAXIMUM_NESTING) {
        fail(s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}")
      } else {
        val newNesting = nesting + 1
        (ty, value) match {
          // simple values
          case (TBuiltin(BTUnit), ValueUnit) =>
            ResultDone(EPrimCon(PCUnit))
          case (TBuiltin(BTBool), ValueBool(b)) =>
            if (b) ResultDone(EPrimCon(PCTrue))
            else ResultDone(EPrimCon(PCFalse))
          case (TBuiltin(BTInt64), ValueInt64(i)) =>
            ResultDone(EPrimLit(PLInt64(i)))
          case (TBuiltin(BTTimestamp), ValueTimestamp(t)) =>
            ResultDone(EPrimLit(PLTimestamp(t)))
          case (TBuiltin(BTDate), ValueDate(t)) =>
            ResultDone(EPrimLit(PLDate(t)))
          case (TBuiltin(BTText), ValueText(t)) =>
            ResultDone(EPrimLit(PLText(t)))
          case (TBuiltin(BTDecimal), ValueDecimal(d)) =>
            ResultDone(EPrimLit(PLDecimal(d)))
          case (TBuiltin(BTParty), ValueParty(p)) =>
            ResultDone(EPrimLit(PLParty(p)))
          case (TContractId(typ), ValueContractId(c)) =>
            typ match {
              case TTyCon(name) => ResultDone(EContractId(c.coid, name))
              case _ => fail(s"Expected a type constructor but found $typ.")
            }

          // optional
          case (TOptional(elemType), ValueOptional(mb)) =>
            mb match {
              case None => ResultDone(ENone(elemType))
              case Some(v0) => go(newNesting, elemType, v0).map(v1 => ESome(elemType, v1))
            }

          // list
          case (TList(elemType), ValueList(ls)) =>
            if (ls.isEmpty) {
              ResultDone(ENil(elemType))
            } else {
              ls.toImmArray
                .traverseU(go(newNesting, elemType, _))
                .map(es => ECons(elemType, es, ENil(elemType)))
            }

          // map
          case (TMap(elemType), ValueMap(map)) =>
            if (map.isEmpty) {
              ResultDone(ETyApp(EBuiltin(BMapInsert), elemType))
            } else {
              ImmArray(map.toList)
                .traverseU {
                  case (key0, value0) => go(newNesting, elemType, value0).map(entry(key0, _))
                }
                .map(l => buildMap(elemType, buildList(tEntry(elemType), l)))
            }

          // variants
          case (TTyConApp(tyCon, tyConArgs), ValueVariant(mbVariantId, constructorName, value)) =>
            val variantId = tyCon
            mbVariantId match {
              case Some(variantId_) if variantId != variantId_ =>
                fail(
                  s"Mismatching variant id, the types tell us $variantId, but the value tells us $variantId_")
              case _ =>
                compiledPackages.getPackage(variantId.packageId) match {
                  // if the package is not there, look it up and restart. stack safe since this will be done
                  // very few times as the cache gets warm. this is also why we do not use the `Result.needDataType`, which
                  // would consume stack regardless
                  case None =>
                    Result.needPackage(
                      variantId.packageId,
                      pkg => {
                        compiledPackages.addPackage(variantId.packageId, pkg).flatMap {
                          case _ => restart
                        }
                      }
                    )
                  case Some(pkg) =>
                    PackageLookup.lookupVariant(pkg, variantId.qualifiedName) match {
                      case Left(err) => ResultError(err)
                      case Right((dataTypParams, DataVariant(variants))) =>
                        variants.find(_._1 == constructorName) match {
                          case None =>
                            fail(
                              s"Couldn't find provided variant constructor $constructorName in variant $variantId")
                          case Some((_, argTyp)) =>
                            if (dataTypParams.length != tyConArgs.length) {
                              sys.error(
                                "TODO(FM) impossible: type constructor applied to wrong number of parameters, this should never happen on a well-typed package, return better error")
                            }
                            val instantiatedArgTyp =
                              replaceParameters(dataTypParams.map(_._1).zip(tyConArgs), argTyp)
                            go(newNesting, instantiatedArgTyp, value).map(e =>
                              EVariantCon(TypeConApp(tyCon, tyConArgs), constructorName, e))
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
                  s"Mismatching record id, the types tell us $recordId, but the value tells us $recordId_")
              case _ =>
                compiledPackages.getPackage(recordId.packageId) match {
                  // if the package is not there, look it up and restart. stack safe since this will be done
                  // very few times as the cache gets warm. this is also why we do not use the `Result.needDataType`, which
                  // would consume stack regardless
                  case None =>
                    Result.needPackage(
                      recordId.packageId,
                      pkg => {
                        compiledPackages.addPackage(recordId.packageId, pkg).flatMap {
                          case _ => restart
                        }
                      }
                    )
                  case Some(pkg) =>
                    PackageLookup.lookupRecord(pkg, recordId.qualifiedName) match {
                      case Left(err) => ResultError(err)
                      case Right((dataTypParams, DataRecord(recordFlds, _mbTpl @ _))) =>
                        if (recordFlds.length != flds.length) {
                          fail(
                            s"Expecting ${recordFlds.length} field for record $recordId, but got ${flds.length}")
                        }
                        if (dataTypParams.length != tyConArgs.length) {
                          sys.error(
                            "TODO(FM) impossible: type constructor applied to wrong number of parameters, this should never happen on a well-typed package, return better error")
                        }
                        val params = dataTypParams.map(_._1).zip(tyConArgs)
                        recordFlds
                          .zip(flds)
                          .traverseU {
                            case ((lbl, typ), (mbLbl, v)) =>
                              mbLbl match {
                                case Some(lbl_) if lbl != lbl_ =>
                                  fail(
                                    s"Mismatching record label $lbl_ (expecting $lbl) for record $recordId")
                                case _ => ()
                              }
                              val replacedTyp = replaceParameters(params, typ)
                              go(newNesting, replacedTyp, v).map(e => (lbl, e))
                          }
                          .map(flds => ERecCon(TypeConApp(tyCon, tyConArgs), flds))
                    }
                }
            }

          // every other pairs of types and values are invalid
          case (otherType, otherValue) =>
            fail(s"mismatching type: $otherType and value: $otherValue")
        }
      }
    }
    exceptionToResultError(go(0, ty0, v0.value))
  }

  private[engine] def translateCreate(
      templateId: Identifier,
      argument: VersionedValue[AbsoluteContractId]): Result[(Type, Expr)] =
    Result.needDataType(
      compiledPackages,
      templateId,
      dataType => {
        // we rely on datatypes which are also templates to have _no_ parameters, according
        // to the DAML-LF spec.
        if (dataType.params.length > 0) {
          ResultError(Error(
            s"Unexpected type parameters ${dataType.params} for template $templateId. Template datatypes should never have parameters."))
        } else {
          val typ = TTyCon(templateId)
          translateValue(typ, argument).map(e => (typ, EUpdate(UpdateCreate(templateId, e))))
        }
      }
    )

  private[engine] def translateFetch(
      templateId: Identifier,
      coid: AbsoluteContractId): Result[(Type, Expr)] =
    Result.needDataType(
      compiledPackages,
      templateId,
      dataType => {
        // we rely on datatypes which are also templates to have _no_ parameters, according
        // to the DAML-LF spec.
        if (dataType.params.length > 0) {
          ResultError(Error(
            s"Unexpected type parameters ${dataType.params} for template $templateId. Template datatypes should never have parameters."))
        } else {
          val typ = TTyCon(templateId)
          ResultDone((typ, EUpdate(UpdateFetch(templateId, EContractId(coid.coid, templateId)))))
        }
      }
    )

  private[engine] def translateExercise(
      templateId: Identifier,
      contractId: AbsoluteContractId,
      choiceId: ChoiceName,
      // actors are either the singleton set of submitter of an exercise command,
      // or the acting parties of an exercise node
      // of a transaction under reconstruction for validation
      actors: Set[Party],
      argument: VersionedValue[AbsoluteContractId]) =
    Result.needTemplate(
      compiledPackages,
      templateId,
      template => {
        template.choices.get(choiceId) match {
          case None =>
            val choicesNames: Seq[String] = template.choices.toList.map(_._1)
            ResultError(Error(
              s"Couldn't find requested choice $choiceId for template $templateId. Available choices: $choicesNames"))
          case Some(choice) =>
            val choiceTyp = choice.argBinder._2
            val actingParties =
              ECons(
                TBuiltin(BTParty),
                ImmArray(actors.map(actor => EPrimLit(PLParty(actor))).toSeq),
                ENil(TBuiltin(BTParty)))
            translateValue(choiceTyp, argument).map(
              e =>
                (
                  choiceTyp,
                  EUpdate(
                    UpdateExercise(
                      templateId,
                      choiceId,
                      EContractId(contractId.coid, templateId),
                      actingParties,
                      e))))
        }
      }
    )

  private[engine] def buildUpdate(bindings: ImmArray[(Type, Expr)]): Expr = {
    bindings.length match {
      case 0 =>
        EUpdate(UpdatePure(TBuiltin(BTUnit), EPrimCon(PCUnit))) // do nothing if we have no commands
      case 1 =>
        bindings(0)._2
      case _ =>
        EUpdate(UpdateBlock(bindings.init.map {
          case (typ, e) => Binding(None, typ, e)
        }, bindings.last._2))
    }
  }

  private[engine] def commandTranslation(cmds: Commands): Result[Expr] = {
    def transformCommand(cmd: Command): Result[(Type, Expr)] = cmd match {
      case CreateCommand(templateId, argument) =>
        translateCreate(templateId, argument)
      case ExerciseCommand(templateId, contractId, choiceId, submitter, argument) =>
        translateExercise(
          templateId,
          AbsoluteContractId(contractId),
          choiceId,
          Set(submitter),
          argument)
    }
    val bindings: Result[ImmArray[(Type, Expr)]] =
      Result.sequence(ImmArray(cmds.commands).map(transformCommand))
    bindings.map(buildUpdate)
  }
}
