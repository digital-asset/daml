// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.lang
import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant
import com.daml.platform.store.backend.common.{Field, TrivialField, TrivialOptionalField}

private[oracle] case class OracleStringField[FROM](extract: FROM => String)
  extends TrivialField[FROM, String] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: String): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleStringOptional[FROM](extract: FROM => Option[String])
  extends TrivialOptionalField[FROM, String] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: String): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleBytea[FROM](extract: FROM => Array[Byte])
  extends TrivialField[FROM, Array[Byte]] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Array[Byte]): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleByteaOptional[FROM](extract: FROM => Option[Array[Byte]])
  extends TrivialOptionalField[FROM, Array[Byte]] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Array[Byte]): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleIntOptional[FROM](extract: FROM => Option[Int])
  extends Field[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: java.lang.Integer): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleBigint[FROM](extract: FROM => Long) extends TrivialField[FROM, Long] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Long): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleSmallintOptional[FROM](extract: FROM => Option[Int])
  extends Field[FROM, Option[Int], java.lang.Integer] {
  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: java.lang.Integer): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleBooleanField[FROM](extract: FROM => Boolean)
  extends TrivialField[FROM, Boolean] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Boolean): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleBooleanOptional[FROM](extract: FROM => Option[Boolean])
  extends Field[FROM, Option[Boolean], java.lang.Boolean] {
  override def convert: Option[Boolean] => lang.Boolean = _.map(Boolean.box).orNull
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: java.lang.Boolean): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleTimestamp[FROM](extract: FROM => Instant)
  extends Field[FROM, Instant, Timestamp] {
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Timestamp): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setTimestamp(index, value)
  }

  override def convert: Instant => java.sql.Timestamp = Timestamp.from
}

private[oracle] case class OracleTimestampOptional[FROM](extract: FROM => Option[Instant])
  extends Field[FROM, Option[Instant], java.sql.Timestamp] {
  override def convert: Option[Instant] => java.sql.Timestamp = _.map(java.sql.Timestamp.from).orNull
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Timestamp): Unit = {
    preparedStatement.setTimestamp(index, value)
  }
}


private[oracle] case class OracleStringArray[FROM](extract: FROM => Iterable[String])
  extends Field[FROM, Iterable[String], Array[String]] {
  override def convert: Iterable[String] => Array[String] = _.toArray
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Array[String]): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}

private[oracle] case class OracleStringArrayOptional[FROM](extract: FROM => Option[Iterable[String]])
  extends Field[FROM, Option[Iterable[String]], Array[String]] {
  override def convert: Option[Iterable[String]] => Array[String] = _.map(_.toArray).orNull
  override def prepareDataTemplate(preparedStatement: PreparedStatement, index: Int, value: Array[String]): Unit = {
    // TODO FIXME either cleanup and use Field from common, or change the line below
    preparedStatement.setObject(index, value)
  }
}
