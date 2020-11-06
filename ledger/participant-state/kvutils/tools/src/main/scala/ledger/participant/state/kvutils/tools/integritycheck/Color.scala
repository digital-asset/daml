// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import scala.io.AnsiColor

final class Color(val text: String) extends AnyVal {
  def green: String = color(AnsiColor.GREEN)

  def red: String = color(AnsiColor.RED)

  def yellow: String = color(AnsiColor.YELLOW)

  def white: String = color(AnsiColor.WHITE)

  private def color(color: String): String =
    color + text + AnsiColor.RESET
}
