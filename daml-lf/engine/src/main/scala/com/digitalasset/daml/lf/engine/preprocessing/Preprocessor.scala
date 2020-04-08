// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import java.util

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.lf.transaction.{GenTransaction, Node, Transaction}
import com.daml.lf.value.Value

import scala.annotation.tailrec
import scala.util.control.NoStackTrace

private[engine] final class Preprocessor(compiledPackages: MutableCompiledPackages) {

  import Preprocessor._
  val transactionPreprocessor = new TransactionPreprocessor(compiledPackages)
  import transactionPreprocessor._
  import commandPreprocessor._
  import valueTranslator.unsafeTranslateValue

  // This pulls all the dependencies of in `typesToProcess0` and `tyConAlreadySeed0`
  private def getDependencies(
      typesToProcess0: List[Ast.Type],
      tmplToProcess0: List[Ref.TypeConName],
      tyConAlreadySeed0: Set[Ref.TypeConName] = Set.empty,
      tmplAlreadySeed0: Set[Ref.TypeConName] = Set.empty,
  ): Result[(Set[Ref.TypeConName], Set[Ref.TypeConName])] = {

    @tailrec
    def go(
        typesToProcess0: List[Ast.Type],
        tmplToProcess0: List[Ref.TypeConName],
        tyConAlreadySeed0: Set[Ref.TypeConName],
        tmplsAlreadySeed0: Set[Ref.TypeConName],
    ): Result[(Set[Ref.TypeConName], Set[Ref.TypeConName])] =
      typesToProcess0 match {
        case typ :: typesToProcess =>
          typ match {
            case Ast.TApp(fun, arg) =>
              go(fun :: arg :: typesToProcess, tmplToProcess0, tyConAlreadySeed0, tmplsAlreadySeed0)
            case Ast.TTyCon(tyCon @ Ref.Identifier(packageId, qualifiedName))
                if !tyConAlreadySeed0(tyCon) =>
              compiledPackages.packages.lift(packageId) match {
                case Some(pkg) =>
                  PackageLookup.lookupDataType(pkg, qualifiedName) match {
                    case Right(Ast.DDataType(_, _, dataType)) =>
                      val typesToProcess = dataType match {
                        case Ast.DataRecord(fields, _) =>
                          fields.foldRight(typesToProcess0)(_._2 :: _)
                        case Ast.DataVariant(variants) =>
                          variants.foldRight(typesToProcess0)(_._2 :: _)
                        case Ast.DataEnum(_) =>
                          typesToProcess0
                      }
                      go(
                        typesToProcess,
                        tmplToProcess0,
                        tyConAlreadySeed0 + tyCon,
                        tmplsAlreadySeed0)
                    case Left(e) =>
                      ResultError(e)
                  }
                case None =>
                  ResultNeedPackage(
                    packageId,
                    _ =>
                      getDependencies(
                        typesToProcess0,
                        tmplToProcess0,
                        tyConAlreadySeed0,
                        tmplsAlreadySeed0))
              }
            case Ast.TTyCon(_) | Ast.TNat(_) | Ast.TBuiltin(_) =>
              go(typesToProcess, tmplToProcess0, tyConAlreadySeed0, tmplsAlreadySeed0)
            case Ast.TVar(_) | Ast.TSynApp(_, _) | Ast.TForall(_, _) | Ast.TStruct(_) =>
              ResultError(Error(s"unserializable type ${typ.pretty}"))
          }
        case Nil =>
          tmplToProcess0 match {
            case tmplId :: tmplsToProcess if tmplsAlreadySeed0(tmplId) =>
              getDependencies(Nil, tmplsToProcess, tyConAlreadySeed0, tmplsAlreadySeed0)
            case tmplId :: tmplsToProcess =>
              val pkgId = tmplId.packageId
              compiledPackages.getPackage(pkgId) match {
                case Some(pkg) =>
                  PackageLookup.lookupTemplate(pkg, tmplId.qualifiedName) match {
                    case Right(template) =>
                      val typs0 = template.choices.map(_._2.argBinder._2).toList
                      val typs1 =
                        if (tyConAlreadySeed0(tmplId)) typs0 else Ast.TTyCon(tmplId) :: typs0
                      val typs2 = template.key.fold(typs1)(_.typ :: typs1)
                      go(typs2, tmplsToProcess, tyConAlreadySeed0, tmplsAlreadySeed0)
                    case Left(error) =>
                      ResultError(error)
                  }
                case None =>
                  ResultNeedPackage(
                    pkgId, {
                      case Some(pkg) =>
                        for {
                          _ <- compiledPackages.addPackage(pkgId, pkg)
                          r <- getDependencies(
                            Nil,
                            tmplToProcess0,
                            tyConAlreadySeed0,
                            tmplsAlreadySeed0)
                        } yield r
                      case None =>
                        ResultError(Error(s"Couldn't find package $pkgId"))
                    }
                  )
              }
            case Nil =>
              ResultDone(tyConAlreadySeed0 -> tmplsAlreadySeed0)
          }
      }

    go(typesToProcess0, tmplToProcess0, tyConAlreadySeed0, tmplAlreadySeed0)
  }

  /**
    * Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value[Value.AbsoluteContractId]): Result[SValue] =
    safelyRun(
      unsafeTranslateValue(ty0, v0),
      getDependencies(List(ty0), List.empty)
    )

  /**
    * Translates  LF commands to a speedy commands.
    */
  def preprocessCommands(
      cmds: data.ImmArray[command.Command],
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(
      unsafePreprocessCommands(cmds),
      getDependencies(List.empty, cmds.map(_.templateId).toList)
    )

  private def getTemplateId(node: Node.GenNode.WithTxValue[Transaction.NodeId, _]) =
    node match {
      case Node.NodeCreate(
          nodeSeed @ _,
          coid @ _,
          coinst,
          optLoc @ _,
          sigs @ _,
          stks @ _,
          key @ _) =>
        coinst.template
      case Node.NodeExercises(
          nodeSeed @ _,
          coid @ _,
          templateId,
          choice @ _,
          optLoc @ _,
          consuming @ _,
          actingParties @ _,
          chosenVal @ _,
          stakeholders @ _,
          signatories @ _,
          controllers @ _,
          children @ _,
          exerciseResult @ _,
          key @ _) =>
        templateId
      case Node.NodeFetch(coid @ _, templateId, _, _, _, _, _) =>
        templateId
      case Node.NodeLookupByKey(templateId, _, key @ _, _) =>
        templateId
    }

  def translateNode[Cid <: Value.ContractId](
      node: Node.GenNode.WithTxValue[Transaction.NodeId, Cid],
  ): Result[speedy.Command] =
    safelyRun(
      unsafeTranslateNode(node),
      getDependencies(List.empty, List(getTemplateId(node)))
    )

  def translateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction.WithTxValue[Transaction.NodeId, Cid],
  ): Result[ImmArray[(Transaction.NodeId, speedy.Command)]] =
    safelyRun(
      unsafeTranslateTransactionRoots(tx),
      getDependencies(List.empty, tx.roots.toList.map(id => getTemplateId(tx.nodes(id))))
    )

}

private[preprocessing] object Preprocessor {

  private[preprocessing] def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  sealed abstract class PreprocessorException extends RuntimeException with NoStackTrace

  // we use the following exceptions for easier error handling in translateValues
  final case class PreprocessorError(err: Error) extends PreprocessorException
  final case class PreprocessorMissingPackage(pkgId: Ref.PackageId) extends PreprocessorException

  @throws[PreprocessorException]
  def fail(s: String): Nothing =
    throw PreprocessorError(ValidationError(s))

  @throws[PreprocessorException]
  def fail(e: Error): Nothing =
    throw PreprocessorError(e)

  @throws[PreprocessorException]
  def assertRight[X](either: Either[Error, X]): X = either match {
    case Left(e) => fail(e)
    case Right(v) => v
  }

  @inline
  def safelyRun[X](
      unsafeRun: => X,
      handleMissingPackages: Result[_]
  ): Result[X] = {

    def start: Result[X] =
      try {
        ResultDone(unsafeRun)
      } catch {
        case PreprocessorError(e) =>
          ResultError(e)
        case PreprocessorMissingPackage(_) =>
          // One package is missing, the we pull all dependencies and restart from scratch.
          handleMissingPackages.flatMap(_ => start)
      }

    start
  }

  @inline
  def safelyRun[X](unsafeRun: => X): Either[Error, X] =
    try {
      Right(unsafeRun)
    } catch {
      case PreprocessorError(e) =>
        Left(e)
      case PreprocessorMissingPackage(pkgId) =>
        Left((Error(s"Couldn't find package $pkgId")))
    }

}
