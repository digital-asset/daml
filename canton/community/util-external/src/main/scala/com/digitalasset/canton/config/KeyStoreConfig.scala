// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

import java.io.File
import scala.annotation.nowarn

/** Configuration for Java keystore with optional password protection. */
final case class KeyStoreConfig(path: File, password: Password)

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
class KeyStoreConfigWriters(confidential: Boolean) {
  val confidentialWriter = new ConfidentialConfigWriter(confidential)

  implicit val passwordWriter: ConfigWriter[Password] =
    confidentialWriter[Password](_.copy(pw = "****"))
  implicit val keyStoreConfigWriter: ConfigWriter[KeyStoreConfig] = deriveWriter[KeyStoreConfig]
}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
object KeyStoreConfig {
  implicit val keyStoreConfigReader: ConfigReader[KeyStoreConfig] = deriveReader[KeyStoreConfig]
}

/** Password wrapper for keystores to prevent the values being printed in logs.
  * @param pw password value - public for supporting PureConfig parsing but callers should prefer accessing through unwrap
  */
final case class Password(pw: String) extends AnyVal {
  def unwrap: String = pw

  def toCharArray: Array[Char] = pw.toCharArray

  // We do not want to print out the password in log files
  override def toString: String = s"Password(****)"
}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
object Password {
  implicit val passwordReader: ConfigReader[Password] = deriveReader[Password]

  def empty: Password = Password("")
}
