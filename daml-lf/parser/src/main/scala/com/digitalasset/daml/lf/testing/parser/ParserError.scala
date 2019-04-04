// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

sealed abstract class ParserError(val description: String) extends RuntimeException(description)

final case class LexingError(override val description: String) extends ParserError(description) {
  override def toString: String = s"ParsingError($description)"
}

final case class ParsingError(override val description: String) extends ParserError(description) {
  override def toString: String = s"ParsingError($description)"
}
