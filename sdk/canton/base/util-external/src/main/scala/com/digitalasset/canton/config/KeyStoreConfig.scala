// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Password wrapper for keystores to prevent the values being printed in logs.
  * @param pw
  *   password value - public for supporting PureConfig parsing but callers should prefer accessing
  *   through unwrap
  */
final case class Password(pw: String) extends AnyVal {
  def unwrap: String = pw

  def toCharArray: Array[Char] = pw.toCharArray

  // We do not want to print out the password in log files
  override def toString: String = s"Password(****)"
}

object Password {
  implicit val passwordReader: ConfigReader[Password] = deriveReader[Password]

  def empty: Password = Password("")
}
