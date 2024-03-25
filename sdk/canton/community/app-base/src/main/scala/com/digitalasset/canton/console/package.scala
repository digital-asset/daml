// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.console.CommandErrors.GenericCommandError

/** General `console` utilities
  */
package object console {

  /** Turn a either into a command result.
    * Left is considered an error, Right is successful.
    */
  implicit class EitherToCommandResultExtensions[A, B](either: Either[A, B]) {
    def toResult(errorDescription: A => String): ConsoleCommandResult[B] =
      either.fold[ConsoleCommandResult[B]](
        err => GenericCommandError(errorDescription(err)),
        CommandSuccessful[B],
      )

    def toResult[Result](
        errorDescription: A => String,
        resultMap: B => Result,
    ): ConsoleCommandResult[Result] =
      either.fold[ConsoleCommandResult[Result]](
        err => GenericCommandError(errorDescription(err)),
        result => CommandSuccessful(resultMap(result)),
      )
  }

  /** Turn an either where Left is a error message into a ConsoleCommandResult.
    */
  implicit class StringErrorEitherToCommandResultExtensions[A](either: Either[String, A]) {
    def toResult: ConsoleCommandResult[A] =
      either.fold[ConsoleCommandResult[A]](GenericCommandError, CommandSuccessful[A])
  }

  /** Strip the Object suffix from the name of the provided class
    */
  def objectClassNameWithoutSuffix(c: Class[_]): String =
    c.getName.stripSuffix("$").replace('$', '.')

}
