// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import scala.util.Failure
import scala.util.Success

private[lf] sealed abstract class Cont[+X, +Q, -A] extends Product with Serializable {
  def flatMap[Y, R >: Q, B <: A](f: X => Cont[Y, R, B]): Cont[Y, R, B]
  def remapQ[R, B](f: Q => Cont[A, R, B]): Cont[X, R, B]
}

object Cont {
  final case class Final[X](x: X) extends Cont[X, Nothing, Any] {
    override def flatMap[Y, R, B](f: X => Cont[Y, R, B]): Cont[Y, R, B] =
      f(x)
    override def remapQ[R, B](f: Nothing => Cont[Any, R, B]): this.type =
      this
  }
  final case class Ask[X, Q, A](q: Q, resume: A => Cont[X, Q, A]) extends Cont[X, Q, A] {
    override def flatMap[Y, R >: Q, B <: A](f: X => Cont[Y, R, B]): Cont[Y, R, B] =
      Ask(q, resume(_).flatMap(f))
    override def remapQ[R, B](f: Q => Cont[A, R, B]): Cont[X, R, B] =
      f(q).flatMap(resume(_).remapQ(f))
  }
}

case class ConversionError(message: String) extends RuntimeException(message)
final case class InterpretationError(error: SError.SError)
    extends RuntimeException(s"${Pretty.prettyError(error).render(80)}")

private[lf] object Free {

  type ErrOr[+X] = Either[RuntimeException, X]

  abstract class Mask {
    type Masked[+_]
    def disguise[X](x: X): Masked[X]
    def reveal[X](x: Masked[X]): X
  }

  val Mask: Mask = new Mask {
    type Masked[+T] = T
    override def disguise[X](x: X): Masked[X] = x
    override def reveal[X](x: Masked[X]): X = x
  }

  type Result[+X, +Q, -A] = Mask.Masked[Cont[ErrOr[X], Option[Q], ErrOr[A]]]

  object Result {

    import Mask._

    private[this] val notAnError = Left(new RuntimeException("Not a Error"))

    type NoQuestion[+X] = Result[X, Nothing, Any]

    def done[X](either: ErrOr[X]): NoQuestion[X] =
      disguise(Cont.Final(either))
    def successful[X](x: X): NoQuestion[X] =
      done(Right(x))
    def failed(err: RuntimeException): NoQuestion[Nothing] = // NoQuestion[Nothing] =
      done(Left(err))
    def question[X, Q, A](q: Q, resume: ErrOr[A] => Result[X, Q, A]): Result[X, Q, A] =
      disguise(Cont.Ask[ErrOr[X], Option[Q], ErrOr[A]](Some(q), resume andThen reveal))
    def interruption[X, Q, A](resume: Any => Result[X, Q, A]): Result[X, Q, A] =
      disguise(Cont.Ask[ErrOr[X], Option[Q], ErrOr[A]](None, resume andThen reveal))

    object Implicits {
      final implicit class ResultOps[X, Q, A](val result: Result[X, Q, A]) extends AnyVal {
        def transform[Y, R >: Q, B <: A](f: ErrOr[X] => Result[Y, R, B]): Result[Y, R, B] =
          disguise(reveal(result).flatMap(f andThen reveal))

        def flatMap[Y, R >: Q, B <: A](f: X => Result[Y, R, B]): Result[Y, R, B] =
          transform(_.fold[Result[Y, R, B]](failed, f))

        def map[Y](f: X => Y): Result[Y, Q, A] =
          flatMap(f andThen successful)

        def remapQ[R, B](f: Q => Result[A, R, B]): Result[X, R, B] =
          disguise(reveal(result).remapQ {
            case None =>
              Cont.Ask[ErrOr[Nothing], None.type, ErrOr[B]](None, _ => Cont.Final(notAnError))
            case Some(q) =>
              reveal(f(q))
          })

        def run(cancel: () => Option[RuntimeException], answer: Q => Result[A, Q, A]): ErrOr[X] = {
          @scala.annotation.tailrec
          def loop(cont: Cont[ErrOr[X], Option[Q], ErrOr[A]]): ErrOr[X] =
            cancel() match {
              case Some(err) =>
                Left(err)
              case None =>
                cont match {
                  case Cont.Final(x) =>
                    x
                  case Cont.Ask(q, resume) =>
                    q match {
                      case None => loop(resume(notAnError))
                      case Some(q) => loop(reveal(answer(q)).flatMap(resume))
                    }
                }
            }

          loop(reveal(result))
        }

        def runFuture(cancel: () => Option[RuntimeException], answer: Q => Future[Result[A, Q, A]])(
            implicit ec: ExecutionContext
        ): Future[X] = {
          def loop(cont: Cont[ErrOr[X], Option[Q], ErrOr[A]]): Future[X] = {
            cancel() match {
              case Some(err) =>
                Future.failed(err)
              case None =>
                cont match {
                  case Cont.Final(x) =>
                    Future.fromTry(x.toTry)
                  case Cont.Ask(q, resume) =>
                    q match {
                      case None => loop(resume(notAnError))
                      case Some(q) =>
                        for {
                          result <- answer(q).transform {
                            case Success(a) => Success(a)
                            case Failure(e: RuntimeException) => Success(Result.failed(e))
                            case Failure(e) => Failure(e)
                          }
                          cont = reveal(result).flatMap(resume)
                          res <- loop(cont)
                        } yield res
                    }
                }
            }
          }

          loop(reveal(result))
        }
      }

      final implicit class ErrOrOps[X](val either: ErrOr[X]) extends AnyVal {
        def toResult: NoQuestion[X] = done(either)
      }
    }
    val Unit: NoQuestion[Unit] = successful(())
  }

  import Result.Implicits._
  import SExpr._, SValue._

  case class Question(
      name: String,
      version: Long,
      payload: SValue,
      private val stackTrace: StackTrace,
  )

  def convError(message: String) = Left(ConversionError(message))

  private final implicit class StringOr[X](val e: Either[String, X]) extends AnyVal {
    def toErrOr: ErrOr[X] = e.left.map(ConversionError(_))
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
    @scala.annotation.nowarn("msg=parameter value x in method loop is never used")
    private[this] def runExpr(expr: SExpr): Result.NoQuestion[SValue] = {
      val machine = newMachine(expr)

      def loop(x: Any): Result.NoQuestion[SValue] =
        machine.run() match {
          case SResult.SResultFinal(v) => Result.successful(v)
          case SResult.SResultError(err) => Result.failed(InterpretationError(err))
          case SResult.SResultInterruption => Result.interruption(loop)
          case SResult.SResultQuestion(nothing) => nothing
        }

      loop(())
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
                  Result.done(
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
                Result.question(question, resume)
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
      md <- compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.flatMap(_.metadata).toList
    } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  }
}
