// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.*
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.google.protobuf.ByteString

/** A class representing either an existing PEM file path or an inlined PEM string for configuration
  * file fields
  */
sealed trait PemFileOrString {
  def pemBytes: ByteString
  def pemStream: java.io.InputStream = pemBytes.newInput()
}

object PemFileOrString {
  implicit def pemFileOrStringCantonConfigValidator: CantonConfigValidator[PemFileOrString] =
    CantonConfigValidator.validateAll
}

/** A class representing an existing PEM file path for configuration file fields
  */
final case class PemFile(pemFile: ExistingFile) extends PemFileOrString {
  override lazy val pemBytes: ByteString =
    ByteString.copyFrom(File(pemFile.unwrap.getAbsolutePath).loadBytes)
}

object PemFile {
  implicit def pemFileCantonConfigValidator: CantonConfigValidator[PemFile] =
    CantonConfigValidator.validateAll
}

/** A class representing an inlined PEM string for configuration file fields
  */
final case class PemString(override val pemBytes: ByteString) extends PemFileOrString {
  lazy val pemString: String = pemBytes.toStringUtf8
}

object PemString {
  def apply(pemString: String): PemString = new PemString(ByteString.copyFromUtf8(pemString))
}
