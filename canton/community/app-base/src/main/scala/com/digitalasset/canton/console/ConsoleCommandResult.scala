// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.Monad
import cats.syntax.alternative.*
import com.daml.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.console.CommandErrors.{CommandError, GenericCommandError}
import com.digitalasset.canton.error.CantonErrorGroups.CommandErrorGroup
import com.digitalasset.canton.error.*
import com.digitalasset.canton.util.ErrorUtil
import org.slf4j.event.Level

import java.time.Duration
import scala.util.{Failure, Success, Try}

/** Response from a console command.
  */
sealed trait ConsoleCommandResult[+A] {
  def toEither: Either[String, A]

  def flatMap[B](f: A => ConsoleCommandResult[B]): ConsoleCommandResult[B] = this match {
    case CommandSuccessful(a) => f(a)
    case err: CommandError => err
  }

  def map[B](f: A => B): ConsoleCommandResult[B] = this match {
    case CommandSuccessful(a) => CommandSuccessful(f(a))
    case err: CommandError => err
  }
}

object ConsoleCommandResult {

  implicit val consoleCommandResultMonad: Monad[ConsoleCommandResult] =
    new Monad[ConsoleCommandResult] {
      override def flatMap[A, B](fa: ConsoleCommandResult[A])(
          f: A => ConsoleCommandResult[B]
      ): ConsoleCommandResult[B] = fa.flatMap(f)

      override def tailRecM[A, B](
          a: A
      )(f: A => ConsoleCommandResult[Either[A, B]]): ConsoleCommandResult[B] = {
        def go(ccr: ConsoleCommandResult[Either[A, B]]): ConsoleCommandResult[B] = ccr match {
          case CommandSuccessful(Left(a)) => go(f(a))
          case CommandSuccessful(Right(b)) => CommandSuccessful(b)
          case err: CommandError => err
        }
        go(CommandSuccessful(Left(a)))
      }

      override def pure[A](x: A): ConsoleCommandResult[A] = CommandSuccessful(x)
    }

  def fromEither[A](either: Either[String, A]): ConsoleCommandResult[A] =
    either match {
      case Left(err) => GenericCommandError(err)
      case Right(value) => CommandSuccessful(value)
    }

  private[console] def runAll[Instance <: InstanceReference, Result](
      instances: Seq[Instance]
  )(
      action: Instance => ConsoleCommandResult[Result]
  )(implicit consoleEnvironment: ConsoleEnvironment): Map[Instance, Result] =
    consoleEnvironment.run {
      forAll(instances)(action)
    }

  /** Call a console command on all instances.
    * Will run all in sequence and will merge all failures.
    * If nothing fails, the final CommandSuccessful result will be returned.
    * @param action Action to perform on instances
    * @return Successful if the action was successful for all instances, otherwise all the errors encountered merged into one.
    */
  private[console] def forAll[Instance <: InstanceReference, Result](
      instances: Seq[Instance]
  )(
      action: Instance => ConsoleCommandResult[Result]
  ): ConsoleCommandResult[Map[Instance, Result]] = {
    val (errors, results) = instances
      .map(instance => instance -> Try(action(instance)))
      .map {
        case (instance, Success(CommandSuccessful(value))) => Right(instance -> value)
        case (instance, Success(err: CommandError)) =>
          Left(
            s"(failure on ${instance.name}): ${err.cause}"
          )
        case (instance, Failure(t)) =>
          Left(s"(exception on ${instance.name}: ${ErrorUtil.messageWithStacktrace(t)}")
      }
      .toList
      .separate
    if (errors.isEmpty) {
      CommandSuccessful(results.toMap)
    } else {
      GenericCommandError(
        s"Command failed on ${errors.length} out of ${instances.length} instances: ${errors.mkString(", ")}"
      )
    }
  }
}

/** Successful command result
  * @param value The value returned from the command
  */
final case class CommandSuccessful[+A](value: A) extends ConsoleCommandResult[A] {
  override lazy val toEither: Either[String, A] = Right(value)
}

object CommandSuccessful {
  def apply(): CommandSuccessful[Unit] = CommandSuccessful(())
}

// Each each in object CommandErrors, will have an error code that begins with `CA12` ('CA1' due to inheritance from CommunityAppError, '2' due to the argument)
object CommandErrors extends CommandErrorGroup {

  sealed trait CommandError extends ConsoleCommandResult[Nothing] {
    override lazy val toEither: Either[String, Nothing] = Left(cause)
    def cause: String
  }

  sealed abstract class CantonCommandError(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends BaseCantonError
      with CommandError

  sealed abstract class CommandErrorCode(id: String, category: ErrorCategory)
      extends ErrorCode(id, category) {
    override def errorConveyanceDocString: Option[String] = Some(
      "These errors are shown as errors on the console."
    )
  }

  object CommandInternalError
      extends CommandErrorCode(
        "CONSOLE_COMMAND_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class ErrorWithException(throwable: Throwable)
        extends CantonCommandError(
          "An internal error has occurred while running a console command.",
          Some(throwable),
        )
    final case class NullError()
        extends CantonCommandError("Console command has returned 'null' as result.")
  }

  // The majority of the use cases of this error are for generic Either[..., ...] => ConsoleCommandResult[...] conversions
  // Thus, it doesn't have an error code because the underlying error that is wrapped should provide the error code
  // TODO(i6183) - replace uses of this wrapper with a CantonParentError wrapper except when parsing gRPC errors
  final case class GenericCommandError(cause: String)
      extends ConsoleCommandResult[Nothing]
      with CommandError

  object ConsoleTimeout
      extends CommandErrorCode(
        "CONSOLE_COMMAND_TIMED_OUT",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(timeout: Duration)
        extends CantonCommandError(s"Condition never became true after ${timeout}")
  }

  object NodeNotStarted
      extends CommandErrorCode(
        "NODE_NOT_STARTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    override def logLevel: Level = Level.ERROR
    final case class ErrorCanton(instance: LocalInstanceReference)
        extends CantonCommandError(s"Instance $instance has not been started. ")
  }

}
