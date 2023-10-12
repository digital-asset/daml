// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import scala.util.control.NoStackTrace

/** Handle an error from a console.
  * We expect this implementation will either throw or exit, hence the [[scala.Nothing]] return type.
  */
trait ConsoleErrorHandler {
  def handleCommandFailure(): Nothing

  def handleInternalError(): Nothing
}

/** @deprecated use [[CommandFailure]] or [[CantonInternalError]] instead.
  */
trait CommandExecutionFailedException extends Throwable

class CommandFailure()
    extends Throwable(s"Command execution failed.")
    with NoStackTrace
    with CommandExecutionFailedException

class CantonInternalError()
    extends Throwable(
      s"Command execution failed due to an internal error. Please file a bug report."
    )
    with NoStackTrace
    with CommandExecutionFailedException

/** Throws a [[CommandFailure]] or [[CantonInternalError]] when a command fails.
  * The throwables do not have a stacktraces, to avoid noise in the interactive console.
  */
object ThrowErrorHandler extends ConsoleErrorHandler {
  override def handleCommandFailure(): Nothing = throw new CommandFailure()

  override def handleInternalError(): Nothing = throw new CantonInternalError()
}
