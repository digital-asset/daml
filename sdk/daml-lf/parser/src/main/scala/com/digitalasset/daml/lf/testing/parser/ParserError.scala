// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import scala.util.parsing.input.Position

private[daml] sealed abstract class ParserError(val description: String)
    extends RuntimeException(description)

private[daml] final case class LexingError(message: String) extends ParserError(message) {
  override def toString: String = s"ParsingError($description)"
}

private[daml] final case class ParsingError(message: String, position: Position)
    extends ParserError(message + s" at line ${position.line}, column ${position.column}") {
  override def toString: String = s"ParsingError($description)"
}
