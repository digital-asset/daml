// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import scala.util.Try

package object console {

  /** Given old state and command arguments, what is the new state? */
  type Action = (State, Seq[String]) => State

  /** Given the full commmand name, print some help */
  type Help = List[String] => Unit

  case class CommandError(message: String, reason: Option[Throwable]) extends Throwable {
    override def getMessage: String = message
  }

  final implicit class CommandTryOps[T](val value: Try[T]) extends AnyVal {
    def ~>(msg: String): Either[CommandError, T] =
      value.toEither.left.map(e => CommandError(msg, Some(e)))
  }

  final implicit class CommandOptionOps[T](val value: Option[T]) extends AnyVal {
    def ~>(msg: String): Either[CommandError, T] = value.toRight(CommandError(msg, None))
  }

  final implicit class CommandEitherOps[T](val value: Either[Throwable, T]) extends AnyVal {
    def ~>(msg: String): Either[CommandError, T] = value.left.map(e => CommandError(msg, Some(e)))
  }
}
