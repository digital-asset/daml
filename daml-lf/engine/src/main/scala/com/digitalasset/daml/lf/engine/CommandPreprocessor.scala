// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import java.util

import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Util._
import com.digitalasset.daml.lf.speedy.{SValue, Command => SpeedyCommand}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

private[engine] object CommandPreprocessor {
  def apply(compiledPackages: ConcurrentCompiledPackages): CommandPreprocessor = {
    new CommandPreprocessor(compiledPackages)
  }

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }
}

private[engine] class CommandPreprocessor(compiledPackages: ConcurrentCompiledPackages) {

  import CommandPreprocessor.ArrayList

  // we use this for easier error handling in translateValues
  private[this] case class CommandPreprocessingException(err: Error)
      extends RuntimeException(err.toString, null, true, false)

  private[this] def fail[A](s: String): A =
    throw CommandPreprocessingException(Error(s))

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
      v0: VersionedValue[AbsoluteContractId]): Result[SValue] = {
    import SValue._
    import scalaz.std.option._
    import scalaz.syntax.traverse.ToTraverseOps

    def exceptionToResultError[A](x: => Result[A]): Result[A] =
      try {
        x
      } catch {
        case CommandPreprocessingException(err) => ResultError(err)
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
          case (TBuiltin(BTUnit), ValueUnit) =>
            ResultDone(SUnit(()))
          case (TBuiltin(BTBool), ValueBool(b)) =>
            ResultDone(SBool(b))
          case (TBuiltin(BTInt64), ValueInt64(i)) =>
            ResultDone(SInt64(i))
          case (TBuiltin(BTTimestamp), ValueTimestamp(t)) =>
            ResultDone(STimestamp(t))
          case (TBuiltin(BTDate), ValueDate(t)) =>
            ResultDone(SDate(t))
          case (TBuiltin(BTText), ValueText(t)) =>
            ResultDone(SText(t))
          case (TBuiltin(BTDecimal), ValueDecimal(d)) =>
            ResultDone(SDecimal(d))
          case (TBuiltin(BTParty), ValueParty(p)) =>
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

          // map
          case (TMap(elemType), ValueMap(map)) =>
            map.toImmArray
              .traverseU {
                case (key0, value0) => go(newNesting, elemType, value0).map(key0 -> _)
              }
              .map(l => SMap(HashMap(l.toSeq: _*)))
          // variants
          case (TTyConApp(tyCon, tyConArgs), ValueVariant(mbVariantId, constructorName, val0)) =>
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
                      compiledPackages.addPackage(variantId.packageId, _).flatMap(_ => restart)
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
                            go(newNesting, instantiatedArgTyp, val0).map(
                              SVariant(tyCon, constructorName, _))
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
                      compiledPackages.addPackage(recordId.packageId, _).flatMap(_ => restart)
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
                          .map(
                            flds =>
                              SRecord(
                                tyCon,
                                flds.iterator.map(_._1).toArray,
                                ArrayList(flds.map(_._2).toSeq: _*)
                            ))
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

  private[engine] def preprocessCreate(
      templateId: Identifier,
      argument: VersionedValue[AbsoluteContractId]): Result[(Type, SpeedyCommand)] =
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
          translateValue(typ, argument).map(typ -> SpeedyCommand.Create(templateId, _))
        }
      }
    )

  private[engine] def preprocessFetch(
      templateId: Identifier,
      coid: AbsoluteContractId): Result[(Type, SpeedyCommand)] =
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
          ResultDone(typ -> SpeedyCommand.Fetch(templateId, SValue.SContractId(coid)))
        }
      }
    )

  private[engine] def preprocessExercise(
      templateId: Identifier,
      contractId: ContractId,
      choiceId: ChoiceName,
      // actors are either the singleton set of submitter of an exercise command,
      // or the acting parties of an exercise node
      // of a transaction under reconstruction for validation
      actors: Set[Party],
      argument: VersionedValue[AbsoluteContractId]): Result[(Type, SpeedyCommand)] =
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
            val actingParties = ImmArray(actors.toSeq.map(actor => SValue.SParty(actor)))
            translateValue(choiceTyp, argument).map(
              choiceTyp -> SpeedyCommand
                .Exercise(templateId, SValue.SContractId(contractId), choiceId, actingParties, _))
        }
      }
    )

  private[engine] def preprocessCreateAndExercise(
      templateId: ValueRef,
      createArgument: VersionedValue[AbsoluteContractId],
      choiceId: String,
      choiceArgument: VersionedValue[AbsoluteContractId],
      actors: Set[Party]): Result[(Type, SpeedyCommand)] = {
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
          translateValue(typ, createArgument).flatMap {
            createValue =>
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
                      val actingParties = ImmArray(actors.toSeq.map(actor => SValue.SParty(actor)))
                      translateValue(choiceTyp, choiceArgument).map(
                        choiceTyp -> SpeedyCommand
                          .CreateAndExercise(templateId, createValue, choiceId, _, actingParties))
                  }
                }
              )
          }
        }
      }
    )
  }

  private[engine] def preprocessCommand(cmd: Command): Result[(Type, SpeedyCommand)] =
    cmd match {
      case CreateCommand(templateId, argument) =>
        preprocessCreate(templateId, argument)
      case ExerciseCommand(templateId, contractId, choiceId, submitter, argument) =>
        preprocessExercise(
          templateId,
          AbsoluteContractId(contractId),
          choiceId,
          Set(submitter),
          argument)
      case CreateAndExerciseCommand(
          templateId,
          createArgument,
          choiceId,
          choiceArgument,
          submitter) =>
        preprocessCreateAndExercise(
          templateId,
          createArgument,
          choiceId,
          choiceArgument,
          Set(submitter))
    }

  private[engine] def preprocessCommands(
      cmds0: Commands): Result[ImmArray[(Type, SpeedyCommand)]] = {
    // before, we had
    //
    // ```
    // Result.sequence(ImmArray(cmds.commands).map(preprocessCommand))
    // ```
    //
    // however that is bad, because it'll generate a `NeedPackage` for each command,
    // if the same package is needed for every command. If we go step by step,
    // on the other hand, we will cache the package and go through with execution
    // after the first command which demands it.
    @tailrec
    def go(
        processed: BackStack[(Type, SpeedyCommand)],
        toProcess: ImmArray[Command]): Result[ImmArray[(Type, SpeedyCommand)]] = {
      toProcess match {
        case ImmArray() => ResultDone(processed.toImmArray)
        case ImmArrayCons(cmd, cmds) =>
          preprocessCommand(cmd) match {
            case ResultDone(processedCommand) => go(processed :+ processedCommand, cmds)
            case ResultError(err) => ResultError(err)
            case ResultNeedContract(acoid, resume) =>
              ResultNeedContract(acoid, { contract =>
                resume(contract).flatMap(processedCommand =>
                  goResume(processed :+ processedCommand, cmds))
              })
            case ResultNeedPackage(pkgId, resume) =>
              ResultNeedPackage(pkgId, { pkg =>
                resume(pkg).flatMap(processedCommand =>
                  goResume(processed :+ processedCommand, cmds))
              })
            case ResultNeedKey(key, resume) =>
              ResultNeedKey(key, { contract =>
                resume(contract).flatMap(processedCommand =>
                  goResume(processed :+ processedCommand, cmds))
              })
          }
      }
    }

    def goResume(processed: BackStack[(Type, SpeedyCommand)], toProcess: ImmArray[Command]) =
      go(processed, toProcess)

    go(BackStack.empty, cmds0.commands)
  }

}
