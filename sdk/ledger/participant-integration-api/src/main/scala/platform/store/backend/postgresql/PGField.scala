// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.common.Field
import com.daml.platform.store.interning.StringInterning

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

private[postgresql] case class PGStringArrayOptional[FROM](
    extract: StringInterning => FROM => Option[Iterable[String]]
) extends PGStringArrayBase[FROM, Option[Iterable[String]]] {
  override def convert: Option[Iterable[String]] => String = _.map(convertBase).orNull
}

private[postgresql] trait PGIntArrayBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')::integer[]"

  protected def convertBase: Iterable[Int] => String = { in =>
    in.mkString("|")
  }
}

private[postgresql] case class PGIntArray[FROM](extract: StringInterning => FROM => Iterable[Int])
    extends PGIntArrayBase[FROM, Iterable[Int]] {
  override def convert: Iterable[Int] => String = convertBase
}

private[postgresql] case class PGIntArrayOptional[FROM](
    extract: StringInterning => FROM => Option[Iterable[Int]]
) extends PGIntArrayBase[FROM, Option[Iterable[Int]]] {
  override def convert: Option[Iterable[Int]] => String = _.map(convertBase).orNull
}

private[postgresql] case class PGSmallintOptional[FROM](
    extract: StringInterning => FROM => Option[Int]
) extends Field[FROM, Option[Int], java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}
