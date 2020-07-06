// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import scala.io.AnsiColor

object Print {
  def green(text: String): Unit =
    color(AnsiColor.GREEN, text)

  def red(text: String): Unit =
    color(AnsiColor.RED, text)

  def white(text: String): Unit =
    color(AnsiColor.WHITE, text)

  def color(color: String, text: String): Unit =
    println(color + text + AnsiColor.RESET)
}
