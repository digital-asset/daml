// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

final case class Error(what: Symbol, message: String) {
  def prettyPrint: String = s"Error: $what, $message"
  def within(another: Symbol): Error = Error(what = another, message = s"($prettyPrint)")
}

final case class JwtException(error: Error) extends RuntimeException
