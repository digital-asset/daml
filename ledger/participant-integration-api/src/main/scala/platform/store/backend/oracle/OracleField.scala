// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant
import com.daml.platform.store.backend.common.Field
import spray.json._
import spray.json.DefaultJsonProtocol._

private[oracle] case class OracleTimestamp[FROM](extract: FROM => Instant)
    extends Field[FROM, Instant, Timestamp] {
  override def convert: Instant => java.sql.Timestamp = Timestamp.from
  override def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: Timestamp,
  ): Unit = {
    preparedStatement.setTimestamp(index, value)
  }
}

private[oracle] case class OracleTimestampOptional[FROM](extract: FROM => Option[Instant])
    extends Field[FROM, Option[Instant], java.sql.Timestamp] {
  override def convert: Option[Instant] => java.sql.Timestamp =
    _.map(java.sql.Timestamp.from).orNull
  override def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: Timestamp,
  ): Unit = {
    preparedStatement.setTimestamp(index, value)
  }
}

private[oracle] case class OracleStringArray[FROM](extract: FROM => Iterable[String])
    extends Field[FROM, Iterable[String], String] {
  override def convert: Iterable[String] => String = _.toList.toJson.compactPrint
  override def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: String,
  ): Unit = {
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleStringArrayOptional[FROM](
    extract: FROM => Option[Iterable[String]]
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
