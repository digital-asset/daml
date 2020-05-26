// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

private[daml] sealed abstract class ParserError(val description: String)
    extends RuntimeException(description)

private[daml] final case class LexingError(override val description: String)
    extends ParserError(description) {
  override def toString: String = s"ParsingError($description)"
}

private[daml] final case class ParsingError(override val description: String)
    extends ParserError(description) {
  override def toString: String = s"ParsingError($description)"
}
