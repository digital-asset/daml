// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

sealed abstract class LanguageMinorVersion extends Product with Serializable {
  import LanguageMinorVersion._
  def toProtoIdentifier: String = this match {
    case Stable(id) => id
    case Dev => "dev"
  }
}

object LanguageMinorVersion {
  final case class Stable(identifier: String) extends LanguageMinorVersion
  case object Dev extends LanguageMinorVersion

  def fromProtoIdentifier(identifier: String): LanguageMinorVersion = identifier match {
    case "dev" => Dev
    case _ => Stable(identifier)
  }

  object Implicits {
    import scala.language.implicitConversions

    implicit def `LMV from proto identifier`(identifier: String): LanguageMinorVersion =
      fromProtoIdentifier(identifier)
  }
}
