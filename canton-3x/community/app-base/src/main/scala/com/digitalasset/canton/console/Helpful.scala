// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

/** Implementors will have a `help` method available that will be callable from the Console.
  * Implementors should annotate appropriate methods with `@Help.Summary` to have them included.
  */
trait Helpful {

  def help()(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
    val featureSet = consoleEnvironment.featureSet
    consoleEnvironment.consoleOutput.info(Help.forInstance(this, scope = featureSet))
  }

  @Help.Summary("Help for specific commands (use help() or help(\"method\") for more information)")
  @Help.Topic(Seq("Top-level Commands"))
  def help(methodName: String)(implicit consoleEnvironment: ConsoleEnvironment): Unit =
    consoleEnvironment.consoleOutput.info(
      Help.forMethod(this, methodName, scope = consoleEnvironment.featureSet)
    )

}
