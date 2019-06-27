// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.language

sealed abstract class LanguageMinorVersion extends ProtoStableDevVersion {
  import LanguageMinorVersion._
  def toProtoIdentifier: String = this match {
    case Stable(id) => id
    case Dev => "dev"
  }
}

object LanguageMinorVersion extends ProtoStableDevVersionCompanion[LanguageMinorVersion] {
  final case class Stable(identifier: String) extends LanguageMinorVersion
  case object Dev extends LanguageMinorVersion

  object Implicits {
    import scala.language.implicitConversions

    implicit def `LMV from proto identifier`(identifier: String): LanguageMinorVersion =
      fromProtoIdentifier(identifier)
  }
}
