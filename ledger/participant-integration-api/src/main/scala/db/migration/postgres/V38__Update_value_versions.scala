// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform
package db.migration
package postgres

import java.sql.{Connection, ResultSet}

import anorm.{BatchSql, NamedParameter}
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

private[migration] final class V38__Update_value_versions extends BaseJavaMigration {

  import com.daml.lf
  import translation.ValueSerializer

  private[this] type Value = lf.value.Value.VersionedValue

  private[this] val BATCH_SIZE = 1000

  private[this] val stableValueVersion = lf.transaction.TransactionVersion.V10
  private[this] val stableValueVersionAsInt = stableValueVersion.protoValue.toInt

  private[this] val SELECT_EVENTS =
    """SELECT
         event_id,
         create_argument,
         create_key_value,
         exercise_argument,
         exercise_result
       FROM
         participant_events
    """

  private[this] val UPDATE_EVENTS =
    """UPDATE participant_events SET
         create_argument = {create_argument},
         create_key_value = {create_key_value},
         exercise_argument = {exercise_argument},
         exercise_result = {exercise_result}
       WHERE
         event_id = {event_id}
    """

  private[this] val SELECT_CONTRACTS =
    """SELECT
         contract_id,
         create_argument
       FROM
         participant_contracts
    """

  private[this] val UPDATE_CONTRACTS =
    """UPDATE participant_contracts SET
         create_argument = {create_argument}
       WHERE
         contract_id = {contract_id}
    """

  private[this] def readValue(
      rows: ResultSet,
      label: String,
  ): (String, Option[Value]) =
    label -> Option(rows.getBinaryStream(label)).map(ValueSerializer.deserializeValue)

  private[this] def valueNeedUpdate(namedValue: (String, Option[Value])): Boolean =
    namedValue._2.exists(_.version.protoValue.toInt < stableValueVersionAsInt)

  private[this] def updateAndSerialize(
      nameValue: (String, Option[Value]),
      tableName: String,
      rowKey: String,
  ): NamedParameter = {
    val (label, oldValue) = nameValue
    val newValue =
      oldValue.map(value =>
        ValueSerializer.serializeValue(
          value = value.copy(stableValueVersion),
          errorContext = s"failed to serialize $label for $tableName $rowKey",
        )
      )
    NamedParameter(label, newValue)
  }

  private[this] def readAndUpdate(
      rows: ResultSet,
      tableName: String,
      rowKeyLabel: String,
      valueLabels: List[String],
  ): List[NamedParameter] = {
    val rowKey = rows.getString(rowKeyLabel)
    val values = valueLabels.map(readValue(rows, _))
    if (values.exists(valueNeedUpdate))
      NamedParameter(rowKeyLabel, rowKey) :: values.map(updateAndSerialize(_, tableName, rowKey))
    else
      List.empty
  }

  private[this] def readAndUpdate(
      sqlQuery: String,
      rowType: String,
      rowKeyLabel: String,
      valueLabels: List[String],
  )(implicit connection: Connection): Iterator[List[NamedParameter]] = {
    val stat = connection.createStatement()
    stat.setFetchSize(BATCH_SIZE)
    val rows: ResultSet = stat.executeQuery(sqlQuery)
    def updates: Iterator[List[NamedParameter]] = new Iterator[List[NamedParameter]] {
      override def hasNext: Boolean = {
        rows.next()
      }

      override def next(): List[NamedParameter] =
        readAndUpdate(rows, rowType, rowKeyLabel, valueLabels)
    }
    updates.filter(_.nonEmpty)
  }

  private[this] def save(
      sqlUpdate: String,
      parameters: Iterator[List[NamedParameter]],
  )(implicit connection: Connection): Unit =
    parameters.grouped(BATCH_SIZE).foreach { case batch =>
      BatchSql(
        sqlUpdate,
        batch.head,
        batch.tail: _*
      ).execute()
    }

  @throws[Exception]
  override def migrate(context: Context): Unit = {
    implicit val connection: Connection = context.getConnection

    save(
      UPDATE_EVENTS,
      readAndUpdate(
        sqlQuery = SELECT_EVENTS,
        rowType = "event",
        rowKeyLabel = "event_id",
        valueLabels =
          List("create_argument", "create_key_value", "exercise_argument", "exercise_result"),
      ),
    )

    save(
      UPDATE_CONTRACTS,
      readAndUpdate(
        sqlQuery = SELECT_CONTRACTS,
        rowType = "contract",
        rowKeyLabel = "contract_id",
        valueLabels = List("create_argument"),
      ),
    )
  }

}
