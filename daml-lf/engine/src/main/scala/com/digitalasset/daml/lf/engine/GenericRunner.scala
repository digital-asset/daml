// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.language.{Ast, PackageInterface, Util => AstUtil}
import com.daml.lf.speedy._
import com.daml.lf.{value => lf}
import com.daml.logging.LoggingContext
import com.daml.nameof.NameOf
import scalaz.std.list._
import scalaz.std.either._
import scalaz.syntax.traverse._

import scala.annotation.tailrec
import scala.util.control.NonFatal

final class GenericRunner private (
    compiledPackages: CompiledPackages,
    possibles: List[GenericRunner.QuestionDescription],
) {

  import GenericRunner._

  private val questionMap =
    possibles.view
      .map(q => q.ref -> q)
      .toMap[Ref.Identifier, GenericRunner.QuestionDescription]

  // We overwrite the definition of for tryCratch and questions
  private val extendedCompiledPackages = {

    new CompiledPackages(Compiler.Config.Dev) {

      val overridedDefMap =
        possibles.foldLeft(Map.empty[SExpr.SDefinitionRef, SDefinition])((acc, q) =>
          acc.updated(
            SExpr.LfDefRef(q.ref),
            SDefinition(SExpr.SEBuiltin(SBuiltin.SBGenericAsk(q.ref))),
          )
        )

      override def getDefinition(dref: SExpr.SDefinitionRef): Option[SDefinition] =
        overridedDefMap.get(dref).orElse(compiledPackages.getDefinition(dref))
      // FIXME: avoid override of non abstract method
      override def pkgInterface: PackageInterface = compiledPackages.pkgInterface
      override def packageIds: collection.Set[Ref.PackageId] = compiledPackages.packageIds
      // FIXME: avoid override of non abstract method
      override def definitions: PartialFunction[SExpr.SDefinitionRef, SDefinition] =
        overridedDefMap.orElse(compiledPackages.definitions)
    }
  }

  private[this] val valueTranslator = new preprocessing.ValueTranslator(
    pkgInterface = extendedCompiledPackages.pkgInterface,
    requireV1ContractIdSuffix = false,
  )

  private[this] def loop(
      machine: Speedy.GenericMachine,
      finalType: Ast.Type,
  ): Result[Value] =
    machine.run() match {
      case SResult.SResultError(err) =>
        Result.Error(err.toString)
      case SResult.SResultFinal(v) =>
        Result.Done(Value(finalType, v))
      case SResult.SResultQuestion(speedy.Question.Generic(tag: Ref.Identifier, arg, callback))
          if questionMap.isDefinedAt(tag) =>
        val description = questionMap(tag)
        Result.Question(
          tag,
          Value(description.argType, arg),
          { answer =>
            if (answer.typ == description.answerType) {
              callback(SExpr.SEValue(answer.svalue))
              loop(machine, finalType)
            } else {
              Result.Error(NameOf.qualifiedNameOfCurrentFunc + " 1")
            }
          },
        )
      case _ => Result.Error(NameOf.qualifiedNameOfCurrentFunc + " 3")
    }

  def run(typ: Ast.Type, expr: Ast.Expr)(implicit
      loggingContext: LoggingContext
  ): Result[Value] = {
    scala.util.Try(compiledPackages.compiler.unsafeCompile(expr)) match {
      case scala.util.Success(expr) =>
        // TODO: check the type of expr
        val machine = new Speedy.GenericMachine(
          sexpr = expr,
          traceLog = Speedy.Machine.newTraceLog,
          warningLog = Speedy.Machine.newWarningLog,
          compiledPackages = extendedCompiledPackages,
          profile = new Profile(),
        )
        loop(machine, typ)
      case scala.util.Failure(err) => Result.Error(err.toString)
    }
  }

  private[this] def toSpeedy(typ: Ast.Type, value: lf.Value): Result[SValue] =
    Result.fromEither(valueTranslator.translateValue(typ, value).left.map(_.message))

  def valueToLf(typ: Ast.Type, value: Value): Result[lf.Value] =
    for {
      value <- Result(value.svalue.toUnnormalizedValue)
      _ <- toSpeedy(typ, value)
    } yield value

  def valueToText(value: Value): Result[String] = value.typ match {
    case AstUtil.TText =>
      value.svalue match {
        case SValue.SText(t) => Result.Done(t)
        case _ =>
          Result.Error(NameOf.qualifiedNameOfCurrentFunc + " 1")
      }
    case _ =>
      Result.Error(NameOf.qualifiedNameOfCurrentFunc + " 1")
  }

  def lfToValue(typ: Ast.Type, value: lf.Value): Result[Value] =
    toSpeedy(typ, value).map(Value(typ, _))

}

object GenericRunner {

  sealed abstract class Result[+A] extends Product with Serializable {
    def map[B](f: A => B): Result[B]
    def flatMap[B](f: A => Result[B]): Result[B]
    final def consume(answer: (Ref.Identifier, Value) => Result[Value]): Either[String, A] = {
      @tailrec
      def loop(r: Result[A]): Either[String, A] = r match {
        case Result.Done(r) => Right(r)
        case Result.Error(err) => Left(err)
        case Result.Question(tag, arg, callback) =>
          loop(answer(tag, arg).flatMap(callback))
      }
      loop(this)
    }
  }

  object Result {
    final case class Done[A](r: A) extends Result[A] {
      override def map[B](f: A => B): Result[B] = Done(f(r))
      override def flatMap[B](f: A => Result[B]): Result[B] = f(r)
    }
    final case class Error(err: String) extends Result[Nothing] {
      override def map[B](f: Nothing => B): Result[B] = this
      override def flatMap[B](f: Nothing => Result[B]): Result[B] = this
    }
    final case class Question[A](
        tag: Ref.TypeConName,
        arg: Value,
        answer: Value => Result[A],
    ) extends Result[A] {
      override def map[B](f: A => B): Result[B] = copy(answer = answer(_).map(f))
      override def flatMap[B](f: A => Result[B]): Result[B] = copy(answer = answer(_).flatMap(f))
    }

    def apply[A](x: => A): Result[A] =
      try {
        Done(x)
      } catch {
        case NonFatal(e) =>
          Error(e.getMessage)
      }

    def fromEither[A](either: Either[String, A]): Result[A] =
      either match {
        case Right(value) => Done(value)
        case Left(err) => Error(err)
      }
  }

  final class Value private (
      val typ: Ast.Type,
      private[engine] val svalue: SValue,
  )

  object Value {
    val Unit = new Value(AstUtil.TUnit, SValue.SUnit)
    private[GenericRunner] def apply(typ: Ast.Type, svalue: SValue) = new Value(typ, svalue)
  }

  private[this] def checkQuestion(
      pkgInterface: PackageInterface,
      ref: Ref.DefinitionRef,
  ) =
    for {
      defn <- pkgInterface.lookupValue(ref).left.map(_.pretty)
      description <- defn.typ match {
        case AstUtil.TFun(q, a) =>
          Right(QuestionDescription(ref, q, a))
        case _ =>
          Left(NameOf.qualifiedNameOfCurrentFunc)
      }
    } yield description

  private case class QuestionDescription(
      ref: Ref.DefinitionRef,
      argType: Ast.Type,
      answerType: Ast.Type,
  )

  def build(
      packages: Map[Ref.PackageId, Ast.Package],
      questions: List[Ref.DefinitionRef],
  ): Either[String, GenericRunner] =
    for {
      compiledPackages <- PureCompiledPackages.build(packages, Compiler.Config.Dev)
      descriptions <- questions.traverseU(checkQuestion(compiledPackages.pkgInterface, _))
    } yield new GenericRunner(
      compiledPackages,
      descriptions,
    )

}
