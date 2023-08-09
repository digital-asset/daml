// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package free

import data.Ref
import speedy._
import SExpr.SExpr
import com.daml.logging.LoggingContext
import scalaz.std.either._
import scalaz.std.vector._
import scalaz.syntax.traverse._

private[lf] sealed abstract class Result[+X, +Q] extends Product with Serializable {
  def transform[Y, R >: Q](f: Result.ErrOr[X] => Result[Y, R]): Result[Y, R]
  def flatMap[Y, R >: Q](f: X => Result[Y, R]): Result[Y, R] =
    transform((err: Result.ErrOr[X]) => err.fold(Result.failed, f))
  def map[Y, R >: Q](f: X => Y): Result[Y, R] = flatMap(x => Result.successful(f(x)))
}

private[lf] object Result {

  type ErrOr[+X] = Either[RuntimeException, X]

  object Implicits {
    final implicit class ErrOrOps[X](val either: ErrOr[X]) extends AnyVal {
      def toResult: Final[X] = Final(either)
    }
  }

  final case class Final[X](x: ErrOr[X]) extends Result[X, Nothing] {
    override def transform[Y, Q](f: ErrOr[X] => Result[Y, Q]): Result[Y, Q] = f(x)
  }

  def successful[X](x: X): Final[X] = Final(Right(x))

  def failed(err: RuntimeException): Final[Nothing] = Final(Left(err))

  final case class Interruption[X, Q](resume: () => Result[X, Q]) extends Result[X, Q] {
    override def transform[Y, R >: Q](f: ErrOr[X] => Result[Y, R]): Result[Y, R] =
      copy(() => resume().transform(f))
  }

  final case class Question[X, Q](
      q: Q,
      private val lfContinue: SValue,
      private val continue: ErrOr[SExpr] => Result[X, Q],
  ) extends Result[X, Q] {

    override def transform[Y, R >: Q](f: ErrOr[X] => Result[Y, R]): Result[Y, R] =
      copy(continue = continue(_).transform(f))

    def resume(result: ErrOr[SExpr]): Result[X, Q] = {
      import SExpr._
      continue(
        result.map(expr =>
          SELet1(
            SEMakeClo(Array(), 1, expr),
            SELet1(
              SEAppAtomic(SELocS(1), Array(SEValue(SValue.SUnit))),
              SEAppAtomic(SEValue(lfContinue), Array(SELocS(1))),
            ),
          )
        )
      )
    }
  }

  object Question {
    def apply[Q](q: Q, continue: SValue): Question[SExpr, Q] =
      new Question(q, continue, Final(_))
  }
}

case class ConversionError(message: String) extends RuntimeException(message)
final case class InterpretationError(error: SError.SError)
    extends RuntimeException(s"${Pretty.prettyError(error).render(80)}")

object Free {

  case class Question(
      name: String,
      version: Long,
      payload: SValue,
      private val stackTrace: StackTrace,
  )
  def convError(message: String) = Left(ConversionError(message))

  private final implicit class StringOr[X](val e: Either[String, X]) extends AnyVal {
    def toErrOr: Result.ErrOr[X] = e.left.map(ConversionError(_))
  }

  def run(
      expr: SExpr,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      loggingContext: LoggingContext,
  ): Result[SValue, Question] =
    new Runner(
      expr,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      loggingContext: LoggingContext,
    ).run()

  private class Runner(
      expr: SExpr,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      loggingContext: LoggingContext,
  ) {

    import Result.ErrOr
    import Result.Implicits._

    def run(): Result[SValue, Question] = {
      import SExpr._, SValue._
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
    private[this] def runExpr(expr: SExpr): Result[SValue, Nothing] = {
      val machine = newMachine(expr)

      def loop: () => Result[SValue, Nothing] = () =>
        machine.run() match {
          case SResult.SResultFinal(v) => Result.successful(v)
          case SResult.SResultError(err) => Result.failed(InterpretationError(err))
          case SResult.SResultInterruption => Result.Interruption[SValue, Nothing](loop)
          case SResult.SResultQuestion(nothing) => nothing
        }

      loop()
    }

    def parseQuestion(v: SValue): ErrOr[Either[SValue, (Question, SValue)]] = {
      import SValue._
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

    private[lf] def runFreeMonad(expr: SExpr): Result[SValue, Question] =
      for {
        fsu <- runExpr(expr)
        free <- parseQuestion(fsu).toResult
        result <- free match {
          case Right((question, continue)) =>
            for {
              expr <- Result.Question(question, continue)
              res <- runFreeMonad(expr)
            } yield res
          case Left(v) =>
            v match {
              case SValue.SRecord(_, _, ArrayList(result, _)) =>
                // Unwrap the Tuple2 we get from the inlined StateT.
                Result.successful(result)
              case _ =>
                convError(s"Expected Tuple2 but got $v").toResult
            }
        }
      } yield result

    private def toSrcLoc(v: SValue): ErrOr[SrcLoc] =
      v match {
        case SValue.SRecord(
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
        case SValue.SInt64(i) => Right(i)
        case _ => convError(s"Expected SInt64 but got ${v.getClass.getSimpleName}")
      }
    }

    def toText(v: SValue): ErrOr[String] = {
      v match {
        case SValue.SText(text) => Right(text)
        case _ => convError(s"Expected SText but got ${v.getClass.getSimpleName}")
      }
    }

    def toLocation(v: SValue): ErrOr[Ref.Location] =
      v match {
        case SValue.SRecord(_, _, ArrayList(definition, loc)) =>
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
        case SValue.SList(frames) =>
          frames.toImmArray.toSeq.to(Vector).traverse(toLocation).map(StackTrace(_))
        case _ =>
          new Throwable().printStackTrace();
          convError(s"Expected SList but got $v")
      }

    // Maps GHC unit ids to LF package ids. Used for location conversion.
    val knownPackages: Map[String, Ref.PackageId] = (for {
      pkgId <- compiledPackages.packageIds
      md <- compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.flatMap(_.metadata).toList
    } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  }
}
