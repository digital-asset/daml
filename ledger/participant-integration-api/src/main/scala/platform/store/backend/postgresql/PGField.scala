// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.daml.platform.store.backend.common.Field

private[postgresql] trait PGTimestampBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::timestamp"

  private val PGTimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  protected def convertBase: Instant => String =
    _.atZone(ZoneOffset.UTC).toLocalDateTime
      .format(PGTimestampFormat)
}

private[postgresql] case class PGTimestamp[FROM](extract: FROM => Instant)
    extends PGTimestampBase[FROM, Instant] {
  override def convert: Instant => String = convertBase
}

private[postgresql] case class PGTimestampOptional[FROM](extract: FROM => Option[Instant])
    extends PGTimestampBase[FROM, Option[Instant]] {
  override def convert: Option[Instant] => String = _.map(convertBase).orNull
}

private[postgresql] trait PGStringArrayBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')"

  protected def convertBase: Iterable[String] => String = { in =>
    assert(
      in.forall(!_.contains("|")),
      s"The following input string(s) contain the character '|', which is not expected: ${in.filter(_.contains("|")).mkString(", ")}",
    )
    in.mkString("|")
  }
}

private[postgresql] case class PGStringArray[FROM](extract: FROM => Iterable[String])
    extends PGStringArrayBase[FROM, Iterable[String]] {
  override def convert: Iterable[String] => String = convertBase
}

private[postgresql] case class PGStringArrayOptional[FROM](
    extract: FROM => Option[Iterable[String]]
) extends PGStringArrayBase[FROM, Option[Iterable[String]]] {
  override def convert: Option[Iterable[String]] => String = _.map(convertBase).orNull
}
