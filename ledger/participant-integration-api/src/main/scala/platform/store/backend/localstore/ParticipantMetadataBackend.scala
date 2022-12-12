// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import anorm.SqlParser.{long, str}
import anorm.{RowParser, SqlStringInterpolation, ~}
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._

import java.sql.Connection

/** Provides primitive backend operations for managing:
  * - annotations of a resource,
  * - resource versioning and concurrent change control.
  */
object ParticipantMetadataBackend {

  private val AnnotationParser: RowParser[(String, String, Long)] =
    str("name") ~ str("val").? ~ long("updated_at") map { case key ~ valueO ~ updateAt =>
      (key, valueO.getOrElse(""), updateAt)
    }

  def addAnnotation(
      annotationsTableName: String
  )(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit = {
    val _ =
      SQL"""
         INSERT INTO #$annotationsTableName (internal_id, name, val, updated_at)
         VALUES (
            $internalId,
            $key,
            $value,
            $updatedAt
         )
         """.executeUpdate()(connection)
  }

  def deleteAnnotations(
      annotationsTableName: String
  )(internalId: Int)(connection: Connection): Unit = {
    val _ = SQL"""
         DELETE FROM #$annotationsTableName
         WHERE
             internal_id = $internalId
       """.executeUpdate()(connection)
  }

  def getAnnotations(
      annotationsTableName: String
  )(internalId: Int)(connection: Connection): Map[String, String] = {
    try {
      SQL"""
         SELECT name, val, updated_at
         FROM #$annotationsTableName
         WHERE
          internal_id = $internalId
       """
        .asVectorOf(AnnotationParser)(connection)
        .iterator
        .map { case (key, value, _) => key -> value }
        .toMap
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }
  }

  /** Invokes a query to increase the version number of a resource if the currently stored version matches the expected value.
    * If there are multiple transactions executing this query then the first transaction will proceed and all the others
    * will wait until the first transaction commits or aborts. This behavior should be obtainable by using Read Committed isolation level.
    *
    * @return True on a successful update.
    *         False when no rows were updated (indicating a wrong internal_id or a wrong expected version number).
    */
  def compareAndIncreaseResourceVersion(tableName: String)(
      internalId: Int,
      expectedResourceVersion: Long,
  )(connection: Connection): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE #$tableName
         SET resource_version = resource_version + 1
         WHERE
             internal_id = ${internalId}
             AND
             resource_version = ${expectedResourceVersion}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  /** Invokes a query to increase the version number of a resource.
    * If there are multiple transactions executing this query then the first transaction will proceed and all the others
    * will wait until the first transaction commits or aborts. This behavior should be obtainable by using Read Committed isolation level.
    *
    * @return True on a successful update.
    *         False when no rows were updated (indicating a wrong internal_id).
    */
  def increaseResourceVersion(tableName: String)(internalId: Int)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated = SQL"""
         UPDATE #$tableName
         SET resource_version  = resource_version + 1
         WHERE
             internal_id = ${internalId}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

}
