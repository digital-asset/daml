// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

/** Interface for output to the Console user.
  */
trait ConsoleOutput {

  /** By default, commands should not output anything to the user. So use this only if it absolutely has to be.
    *
    * In particular:
    * - If there is an error, then report this to the log file. The log file will also be displayed to the user.
    * - If a command completes successfully, do not output anything.
    * - If a command returns some `value`, then make the command return the value (instead of printing the value to the console).
    *   This allows the user to access the value programmatically.
    *   (Make sure that `value.toString` creates a readable representation.)
    */
  def info(message: String): Unit
}

/** Logs directly to stdout and stderr.
  */
object StandardConsoleOutput extends ConsoleOutput {
  override def info(message: String): Unit = Console.out.println(message)
}
