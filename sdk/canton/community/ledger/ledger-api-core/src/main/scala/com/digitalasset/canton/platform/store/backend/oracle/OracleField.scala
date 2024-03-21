// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.platform.store.backend.common.Field
import com.digitalasset.canton.platform.store.interning.StringInterning
import spray.json.DefaultJsonProtocol.*
import spray.json.*

import java.sql.PreparedStatement

private[oracle] final case class OracleStringArrayOptional[FROM](
    extract: StringInterning => FROM => Option[Iterable[String]]
) extends Field[FROM, Option[Iterable[String]], String] {
  override def convert: Option[Iterable[String]] => String =
    _.map(_.toList.toJson.compactPrint).getOrElse("[]")
  override def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: String,
  ): Unit = {
    preparedStatement.setObject(index, value)
  }
}

private[oracle] final case class OracleIntArray[FROM](
    extract: StringInterning => FROM => Iterable[Int]
) extends Field[FROM, Iterable[Int], String] {
  override def convert: Iterable[Int] => String = _.toList.toJson.compactPrint
  override def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: String,
  ): Unit = {
    preparedStatement.setObject(index, value)
  }
}

private[oracle] final case class OracleIntArrayOptional[FROM](
    extract: StringInterning => FROM => Option[Iterable[Int]]
) extends Field[FROM, Option[Iterable[Int]], String] {
  override def convert: Option[Iterable[Int]] => String =
    _.map(_.toList.toJson.compactPrint).getOrElse("[]")
  override def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: String,
  ): Unit = {
    preparedStatement.setObject(index, value)
  }
}
