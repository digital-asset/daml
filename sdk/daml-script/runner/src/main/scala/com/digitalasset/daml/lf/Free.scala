// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package free

import data.Ref
import speedy._
import com.daml.logging.LoggingContext
import scalaz.std.either._
import scalaz.std.vector._
import scalaz.syntax.traverse._

import scala.concurrent.{ExecutionContext, Future}

case class ConversionError(message: String) extends RuntimeException(message)
final case class InterpretationError(error: SError.SError)
    extends RuntimeException(s"${Pretty.prettyError(error).render(80)}")

private[lf] object Free {

  type ErrOr[+X] = Either[RuntimeException, X]

  sealed abstract class Result[+X, +Q, -A] extends Product with Serializable {
    def transform[Y, R >: Q, B <: A](f: ErrOr[X] => Result[Y, R, B]): Result[Y, R, B]

    def flatMap[Y, R >: Q, B <: A](f: X => Result[Y, R, B]): Result[Y, R, B] =
      transform(_.fold(Result.failed, f))

    def map[Y](f: X => Y): Result[Y, Q, A] =
      flatMap(f andThen Result.successful)

    def remapQ[R, B](f: Q => Result[A, R, B]): Result[X, R, B]

    def run[R >: Q, B <: A](
        answer: R => Result[B, R, B],
        canceled: () => Option[RuntimeException],
    ): ErrOr[X] = {
      @scala.annotation.tailrec
      def loop(result: Result[X, R, B]): ErrOr[X] =
        canceled() match {
          case Some(err) =>
            Left(err)
          case None =>
            result match {
              case Result.Final(x) =>
                x
              case Result.Ask(q, resume) =>
                loop(answer(q).transform(resume))
              case Result.Interruption(resume) =>
                loop(resume())
            }
        }

      loop(this)
    }

    def runF[R >: Q, B <: A](
        answer: R => Future[Result[B, R, B]],
        canceled: () => Option[RuntimeException],
    )(implicit ec: ExecutionContext): Future[X] = {
      def loop(cont: Result[X, R, B]): Future[X] =
        canceled() match {
          case Some(err) =>
            Future.failed(err)
          case None =>
            cont match {
              case Result.Final(x) =>
                Future.fromTry(x.toTry)
              case Result.Ask(q, resume) =>
                answer(q).flatMap(x => loop(x.transform(resume)))
              case Result.Interruption(resume) =>
                loop(resume())
            }
        }

      loop(this)
    }
  }

  object Result {

    type NoQuestion[+X] = Result[X, Nothing, Any]

    final case class Final[+X](x: ErrOr[X]) extends NoQuestion[X] {
      override def transform[Y, R, B](f: ErrOr[X] => Result[Y, R, B]): Result[Y, R, B] = f(x)

      override def remapQ[R, B](f: Nothing => Result[Any, R, B]): this.type = this
    }

    def successful[X](x: X): Final[X] = Final(Right(x))

    def failed(err: RuntimeException): Final[Nothing] = Final(Left(err))

    final case class Ask[+X, +Q, -A](q: Q, answer: ErrOr[A] => Result[X, Q, A])
        extends Result[X, Q, A] {
      override def transform[Y, R >: Q, B <: A](f: ErrOr[X] => Result[Y, R, B]): Result[Y, R, B] =
        Ask(q, answer(_).transform(f))

      override def remapQ[R, B](f: Q => Result[A, R, B]): Result[X, R, B] =
        f(q).transform(answer(_).remapQ(f))
    }

    final case class Interruption[+X, +Q, -A](resume: () => Result[X, Q, A])
        extends Result[X, Q, A] {
      override def transform[Y, R >: Q, B <: A](f: ErrOr[X] => Result[Y, R, B]): Result[Y, R, B] =
        Interruption(() => resume().transform(f))

      override def remapQ[R, B](f: Q => Result[A, R, B]): Result[X, R, B] =
        Interruption(() => resume().remapQ(f))
    }

    object Implicits {
      final implicit class ErrOrOps[X](val either: ErrOr[X]) extends AnyVal {
        def toResult: Final[X] = Final(either)
      }
    }

    val Unit: Final[Unit] = successful(())
  }

  import SExpr._, SValue._, Result.Implicits._

  case class Question(
      name: String,
      version: Long,
      payload: SValue,
      private val stackTrace: StackTrace,
  )

  def convError(message: String) = Left(ConversionError(message))

  private final implicit class StringOr[X](val e: Either[String, X]) extends AnyVal {
    def toErrOr: ErrOr[X] = e.left.map(ConversionError)
  }

  def getResult(
      expr: SExpr,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      loggingContext: LoggingContext,
  ): Result[SValue, Question, SExpr] =
    new Runner(
      expr,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      loggingContext: LoggingContext,
    ).getResult()

  private class Runner(
      expr: SExpr,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      loggingContext: LoggingContext,
  ) {

    def getResult(): Result[SValue, Question, SExpr] = {
      for {
        v <- runExpr(expr)
        w <- v match {
          // Unwrap Script type and apply to ()
          // Second value in record is dummy unit, ignored
          case SRecord(_, _, ArrayList(expr @ SPAP(_, _, _), _)) =>
            runFreeMonad(SEAppAtomic(SEValue(expr), Array(SEValue(SUnit))))
          case v =>
            convError(s"Expected record with 1 field but got $v").toResult
        }
      } yield w
    }

    private[this] def newMachine(expr: SExpr): Speedy.PureMachine =
      Speedy.Machine.fromPureSExpr(
        compiledPackages,
        expr,
        iterationsBetweenInterruptions = 100000,
        traceLog = traceLog,
        warningLog = warningLog,
        profile = profile,
      )(loggingContext)

    @scala.annotation.nowarn("msg=dead code following this construct")
    private[this] def runExpr(expr: SExpr): Result.NoQuestion[SValue] = {
      val machine = newMachine(expr)

      def loop: Result.NoQuestion[SValue] =
        machine.run() match {
          case SResult.SResultFinal(v) => Result.successful(v)
          case SResult.SResultError(err) => Result.failed(InterpretationError(err))
          case SResult.SResultInterruption => Result.Interruption(() => loop)
          case SResult.SResultQuestion(nothing) => nothing
        }

      loop
    }

    def parseQuestion(v: SValue): ErrOr[Either[SValue, (Question, SValue)]] = {
      v match {
        case SVariant(
              _,
              "Free",
              _,
              SRecord(
                _,
                _,
                ArrayList(
                  SRecord(
                    _,
                    _,
                    ArrayList(SText(name), SInt64(version), payload, locations, continue),
                  )
                ),
              ),
            ) =>
          for {
            stackTrace <- toStackTrace(locations)
          } yield Right(Question(name, version, payload, stackTrace) -> continue)
        case SVariant(_, "Pure", _, v) =>
          Right(Left(v))
        case _ =>
          convError(s"Expected Free Question or Pure, got $v")
      }
    }

    private[lf] def runFreeMonad(expr: SExpr): Result[SValue, Question, SExpr] = {
      for {
        fsu <- runExpr(expr)
        free <- parseQuestion(fsu).toResult
        result <- free match {
          case Right((question, continue)) =>
            for {
              expr <- {
                def resume(expr: ErrOr[SExpr]): Result.NoQuestion[SExpr] =
                  Result.Final(
                    expr.map(expr =>
                      SELet1(
                        SEMakeClo(Array(), 1, expr),
                        SELet1(
                          SEAppAtomic(SELocS(1), Array(SEValue(SValue.Unit))),
                          SEAppAtomic(SEValue(continue), Array(SELocS(1))),
                        ),
                      )
                    )
                  )
                Result.Ask(question, resume)
              }
              res <- runFreeMonad(expr)
            } yield res
          case Left(v) =>
            v match {
              case SRecord(_, _, ArrayList(result, _)) =>
                // Unwrap the Tuple2 we get from the inlined StateT.
                Result.successful(result)
              case _ =>
                convError(s"Expected Tuple2 but got $v").toResult
            }
        }
      } yield result
    }

    private def toSrcLoc(v: SValue): ErrOr[SrcLoc] =
      v match {
        case SRecord(
              _,
              _,
              ArrayList(unitId, module, file @ _, startLine, startCol, endLine, endCol),
            ) =>
          for {
            unitId <- toText(unitId)
            packageId <- unitId match {
              // GHC uses unit-id "main" for the current package,
              // but the scenario context expects "-homePackageId-".
              case "main" => Ref.PackageId.fromString("-homePackageId-").toErrOr
              case id => knownPackages.get(id).toRight(s"Unknown package $id").toErrOr
            }
            moduleText <- toText(module)
            module <- Ref.ModuleName.fromString(moduleText).toErrOr
            startLine <- toInt(startLine)
            startCol <- toInt(startCol)
            endLine <- toInt(endLine)
            endCol <- toInt(endCol)
          } yield SrcLoc(packageId, module, (startLine, startCol), (endLine, endCol))
        case _ => convError(s"Expected SrcLoc but got $v")
      }

    def toInt(v: SValue): ErrOr[Int] =
      toLong(v).map(_.toInt)

    def toLong(v: SValue): ErrOr[Long] = {
      v match {
        case SInt64(i) => Right(i)
        case _ => convError(s"Expected SInt64 but got ${v.getClass.getSimpleName}")
      }
    }

    def toText(v: SValue): ErrOr[String] = {
      v match {
        case SText(text) => Right(text)
        case _ => convError(s"Expected SText but got ${v.getClass.getSimpleName}")
      }
    }

    def toLocation(v: SValue): ErrOr[Ref.Location] =
      v match {
        case SRecord(_, _, ArrayList(definition, loc)) =>
          for {
            // TODO[AH] This should be the outer definition. E.g. `main` in `main = do submit ...`.
            //   However, the call-stack only gives us access to the inner definition, `submit` in this case.
            //   The definition is not used when pretty printing locations. So, we can ignore this for now.
            definition <- toText(definition)
            loc <- toSrcLoc(loc)
          } yield Ref.Location(loc.pkgId, loc.module, definition, loc.start, loc.end)
        case _ => convError(s"Expected (Text, SrcLoc) but got $v")
      }

    def toStackTrace(
        v: SValue
    ): ErrOr[StackTrace] =
      v match {
        case SList(frames) =>
          frames.toImmArray.toSeq.to(Vector).traverse(toLocation).map(StackTrace(_))
        case _ =>
          new Throwable().printStackTrace();
          convError(s"Expected SList but got $v")
      }

    // Maps GHC unit ids to LF package ids. Used for location conversion.
    val knownPackages: Map[String, Ref.PackageId] = (for {
      pkgId <- compiledPackages.packageIds
      md <- compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.map(_.metadata).toList
    } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  }
}
