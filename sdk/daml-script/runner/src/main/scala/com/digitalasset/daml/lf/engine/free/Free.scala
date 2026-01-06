// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package free

import com.daml.logging.LoggingContext
import data.{ImmArray, Ref}
import speedy.{Pretty, SError}
import ScriptEngine.{
  ExtendedValue,
  ExtendedValueClosureBlob,
  ExtendedValueComputationMode,
  runExtendedValueComputation,
  TraceLog,
  WarningLog,
}
import value.Value._
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
        answer: R => Result[B, R, B]
    ): ErrOr[X] = {
      @scala.annotation.tailrec
      def loop(result: Result[X, R, B]): ErrOr[X] =
        result match {
          case Result.Final(x) =>
            x
          case Result.Ask(q, resume) =>
            loop(answer(q).transform(resume))
        }

      loop(this)
    }

    def runF[R >: Q, B <: A](
        answer: R => Future[Result[B, R, B]]
    )(implicit ec: ExecutionContext): Future[X] = {
      def loop(cont: Result[X, R, B]): Future[X] =
        cont match {
          case Result.Final(x) =>
            Future.fromTry(x.toTry)
          case Result.Ask(q, resume) =>
            answer(q).flatMap(x => loop(x.transform(resume)))
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

    object Implicits {
      final implicit class ErrOrOps[X](val either: ErrOr[X]) extends AnyVal {
        def toResult: Final[X] = Final(either)
      }
    }

    val Unit: Final[Unit] = successful(())
  }

  import Result.Implicits._

  case class Question(
      name: String,
      version: Long,
      payload: ExtendedValue,
      private val stackTrace: StackTrace,
  )

  def convError(message: String) = Left(ConversionError(message))

  private final implicit class StringOr[X](val e: Either[String, X]) extends AnyVal {
    def toErrOr: ErrOr[X] = e.left.map(ConversionError)
  }

  def getResult(
      freeClosure: ExtendedValueClosureBlob, // LF Type: () -> Free ScriptF (a, ())
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      loggingContext: LoggingContext,
      convertLegacyExceptions: Boolean,
  ): Result[ExtendedValue, Question, ExtendedValue] =
    new Runner(
      freeClosure,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      loggingContext: LoggingContext,
      convertLegacyExceptions,
    ).getResult()

  def getResultF(
      freeClosure: ExtendedValueClosureBlob, // LF Type: () -> Free ScriptF (a, ())
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      loggingContext: LoggingContext,
      convertLegacyExceptions: Boolean,
      cancelled: () => Option[RuntimeException],
  )(implicit ec: ExecutionContext): Future[Result[ExtendedValue, Question, ExtendedValue]] =
    new Runner(
      freeClosure: ExtendedValueClosureBlob,
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      loggingContext: LoggingContext,
      convertLegacyExceptions,
      cancelled,
    ).getResultF()

  private class Runner(
      freeClosure: ExtendedValueClosureBlob, // LF Type: () -> Free ScriptF (a, ())
      compiledPackages: CompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
      loggingContext: LoggingContext,
      convertLegacyExceptions: Boolean,
      cancelled: () => Option[RuntimeException] = () => None,
  ) {

    def getResult(): Result[ExtendedValue, Question, ExtendedValue] = {
      for {
        v <- runClosure(freeClosure, List(ValueUnit))
        w <- runFreeMonad(v)
      } yield w
    }

    def getResultF()(implicit
        ec: ExecutionContext
    ): Future[Result[ExtendedValue, Question, ExtendedValue]] =
      Future { getResult() }

    private[this] def runClosure(
        closure: ExtendedValueClosureBlob,
        args: List[ExtendedValue],
    ): Result.NoQuestion[ExtendedValue] =
      runExtendedValueComputation(
        computationMode = ExtendedValueComputationMode.ByClosure(closure, args),
        cancelled = cancelled,
        compiledPackages = compiledPackages,
        iterationsBetweenInterruptions = 100000,
        traceLog = traceLog,
        warningLog = warningLog,
        convertLegacyExceptions = convertLegacyExceptions,
      )(loggingContext).fold(
        err => Result.failed(err.fold(identity, free.InterpretationError(_))),
        Result.successful(_),
      )

    def parseQuestion(
        v: ExtendedValue
    ): ErrOr[Either[ExtendedValue, (Question, ExtendedValueClosureBlob)]] = {
      v match {
        case ValueVariant(
              _,
              "Free",
              ValueRecord(
                _,
                ImmArray(
                  (
                    _,
                    ValueRecord(
                      _,
                      ImmArray(
                        (_, ValueText(name)),
                        (_, ValueInt64(version)),
                        (_, payload),
                        (_, locations),
                        (_, continue: ExtendedValueClosureBlob),
                      ),
                    ),
                  )
                ),
              ),
            ) =>
          for {
            stackTrace <- toStackTrace(locations)
          } yield Right(Question(name, version, payload, stackTrace) -> continue)
        case ValueVariant(_, "Pure", v) => Right(Left(v))
        case _ =>
          convError(s"Expected Free Question or Pure, got $v")
      }
    }

    private[lf] def runFreeMonad(
        freeValue: ExtendedValue
    ): Result[ExtendedValue, Question, ExtendedValue] = {
      for {
        free <- parseQuestion(freeValue).toResult
        result <- free match {
          case Right((question, continue)) =>
            for {
              nextFreeValue <- {
                def resume(answer: ErrOr[ExtendedValue]): Result.NoQuestion[ExtendedValue] =
                  answer.fold(Result.failed, answer => runClosure(continue, List(answer)))
                Result.Ask(question, resume)
              }
              res <- runFreeMonad(nextFreeValue)
            } yield res
          case Left(v) =>
            v match {
              case ValueRecord(_, ImmArray((_, result), _)) =>
                // Unwrap the Tuple2 we get from the inlined StateT.
                Result.successful(result)
              case _ =>
                convError(s"Expected Tuple2 but got $v").toResult
            }
        }
      } yield result
    }

    private def toSrcLoc(v: ExtendedValue): ErrOr[SrcLoc] =
      v match {
        case ValueRecord(
              _,
              ImmArray(
                (_, unitId),
                (_, module),
                _,
                (_, startLine),
                (_, startCol),
                (_, endLine),
                (_, endCol),
              ),
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

    def toInt(v: ExtendedValue): ErrOr[Int] =
      toLong(v).map(_.toInt)

    def toLong(v: ExtendedValue): ErrOr[Long] = {
      v match {
        case ValueInt64(i) => Right(i)
        case _ => convError(s"Expected SInt64 but got ${v.getClass.getSimpleName}")
      }
    }

    def toText(v: ExtendedValue): ErrOr[String] = {
      v match {
        case ValueText(text) => Right(text)
        case _ => convError(s"Expected ValueText but got ${v.getClass.getSimpleName}")
      }
    }

    def toLocation(v: ExtendedValue): ErrOr[Ref.Location] =
      v match {
        case ValueRecord(_, ImmArray((_, definition), (_, loc))) =>
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
        v: ExtendedValue
    ): ErrOr[StackTrace] =
      v match {
        case ValueList(frames) =>
          frames.toImmArray.toSeq.to(Vector).traverse(toLocation).map(StackTrace(_))
        case _ =>
          new Throwable().printStackTrace();
          convError(s"Expected ValueList but got $v")
      }

    // Maps GHC unit ids to LF package ids. Used for location conversion.
    val knownPackages: Map[String, Ref.PackageId] = (for {
      entry <- compiledPackages.signatures.view
      (pkgId, pkg) = entry
    } yield (pkg.metadata.nameDashVersion -> pkgId)).toMap

  }
}
