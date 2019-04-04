// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers.postgresql

import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.iface.reader.InterfaceType
import com.digitalasset.daml.lf.iface.Record
import com.digitalasset.extractor.ledger.LedgerReader.PackageStore
import com.digitalasset.extractor.ledger.types._
import com.digitalasset.extractor.logging.Logging
import com.digitalasset.extractor.Types.{DataIntegrityError, FullyAppliedType}
import com.digitalasset.extractor.Types.FullyAppliedType._
import com.digitalasset.extractor.Types.FullyAppliedType.ops._
import com.digitalasset.extractor.writers.postgresql.DataFormat.TemplateInfo
import com.digitalasset.extractor.writers.postgresql.DataFormatState.MultiTableState
import com.digitalasset.extractor.writers.Writer.RefreshPackages

import cats.implicits._
import doobie.implicits._
import doobie.free.connection
import doobie.free.connection.ConnectionIO

import scala.annotation.tailrec
import scalaz._
import Scalaz._

class MultiTableDataFormat(
    /*splat: Int, */ schemaPerPackage: Boolean,
    mergeIdentical: Boolean,
    stripPrefix: Option[String]
) extends DataFormat[MultiTableState]
    with Logging {
  import Queries._
  import Queries.MultiTable._

  // schema name for contract tables when not using separate schemas per package
  private val singleSchemaName = "template"

  def init(): ConnectionIO[Unit] = {
    createSchema(singleSchemaName).update.run.void
  }

  def handleTemplate(
      state: MultiTableState,
      packageStore: PackageStore,
      template: TemplateInfo
  ): (MultiTableState, ConnectionIO[Unit]) = {
    val (id, params) = template

    val strippedTemplate =
      stripPrefix.map(p => id.name.stripPrefix(p).stripPrefix(".")).getOrElse(id.name)

    val baseName = strippedTemplate
      .take(63)
      .replace(".", "_")
      .replace(":", "_")
      .toLowerCase

    // maybe there's a table for an identical template we can and want merge into?
    val maybeMergeInto = findTableOfIdenticalTemplate(state, packageStore, template)

    maybeMergeInto.fold {
      // base case when there's nothing to merge into

      val takenNames = if (schemaPerPackage) {
        state.templateToTable
          .filterKeys(_.packageId == id.packageId)
          .values
          .map(_.withOutSchema)
      } else {
        state.templateToTable.values.map(_.withOutSchema)
      }

      val tableName = uniqueName(takenNames.toSet, baseName)

      val schemaOrErr: String \/ Option[String] = if (schemaPerPackage) {
        state.packageIdToNameSpace.get(id.packageId) match {
          case s @ Some(_) => s.right
          case None =>
            // This shouldn't happen as [[handlePackageId]] must have been already called
            (s"Couldn't find schema name for package id `${id.packageId}` " +
              s"in known schemas: ${state.packageIdToNameSpace}").left
        }
      } else None.right

      schemaOrErr.fold(
        e => (state, connection.raiseError[Unit](DataIntegrityError(e))), { schemaOpt =>
          val schema = schemaOpt.getOrElse(singleSchemaName)
          val tableNames = TableName(s"${schema}.${tableName}", tableName)

          val io = createIOForTable(tableNames.withSchema, params, id)
          val updatedState = state.copy(
            templateToTable = state.templateToTable + (id -> tableNames)
          )
          (updatedState, io)
        }
      )
    } { mergeIntoTable =>
      // we're reusing the table created for an identical template
      val mergedTemplateIds = state.templateToTable
        .filter(_._2 == mergeIntoTable)
        .keySet + id

      val io = setTableComment(
        mergeIntoTable.withSchema,
        s"Template IDs:\n${mergedTemplateIds.map(idAsComment).mkString("\n")}"
      ).update.run.void

      val updatedState = state.copy(
        templateToTable = state.templateToTable + (id -> mergeIntoTable)
      )
      (updatedState, io)
    }
  }

  private def findTableOfIdenticalTemplate(
      state: MultiTableState,
      packageStore: PackageStore,
      currentTemplate: TemplateInfo
  ): Option[TableName] = {
    if (mergeIdentical) {
      val knownTemplateIds = state.templateToTable.keySet

      val knownTemplateDefs = packageStore.packages
        .map {
          case (packageId, iface) =>
            iface.typeDecls.collect {
              case (id, InterfaceType.Template(r, _)) =>
                Identifier(packageId, id.qualifiedName) -> r
            }
        }
        .foldLeft(Map.empty[Identifier, Record.FWT])(_ ++ _)
        .filterKeys(knownTemplateIds.contains)

      val (thisId, thisParams) = currentTemplate

      for {
        matched <- knownTemplateDefs.find {
          case (thatId, thatParams) =>
            thatId.name == thisId.name && thatParams == thisParams
        }
        matchedId = matched._1
        tableOfMatched <- state.templateToTable.get(matchedId)
      } yield tableOfMatched
    } else {
      None
    }
  }

  def handlePackageId(
      state: MultiTableState,
      packageId: String): (MultiTableState, ConnectionIO[Unit]) = {
    if (schemaPerPackage) {
      val baseName = "package_" + packageId.take(40)
      val schemaName = uniqueName(state.packageIdToNameSpace.values.toSet, baseName)

      val io = createSchema(schemaName).update.run *>
        setSchemaComment(schemaName, s"Package ID: ${packageId}").update.run.void
      val updatedState =
        state.copy(packageIdToNameSpace = state.packageIdToNameSpace + (packageId -> schemaName))

      (updatedState, io)
    } else {
      (state, connection.pure(()))
    }
  }

  def handleExercisedEvent(
      state: MultiTableState,
      transaction: TransactionTree,
      event: ExercisedEvent
  ): RefreshPackages \/ ConnectionIO[Unit] = {
    for {
      table <- state.templateToTable.get(event.templateId).\/>(RefreshPackages(event.templateId))
    } yield {
      val update =
        if (event.consuming)
          setContractArchived(
            table.withSchema,
            event.contractCreatingEventId,
            transaction.transactionId,
            event.eventId).update.run.void
        else
          connection.pure(())

      val insert =
        insertExercise(
          event,
          transaction.transactionId,
          transaction.rootEventIds.contains(event.eventId)).update.run.void

      update *> insert
    }
  }

  def handleCreatedEvent(
      state: MultiTableState,
      transaction: TransactionTree,
      event: CreatedEvent
  ): RefreshPackages \/ ConnectionIO[Unit] = {
    for {
      table <- state.templateToTable.get(event.templateId).\/>(RefreshPackages(event.templateId))
    } yield {
      val insertQuery = insertContract(
        table.withSchema,
        event,
        transaction.transactionId,
        transaction.rootEventIds.contains(event.eventId)
      )

      insertQuery.update.run.void
    }
  }

  private def createIOForTable(
      tableName: String,
      params: iface.Record[(String, iface.Type)],
      templateId: Identifier
  ): ConnectionIO[Unit] = {
    val drop = dropTableIfExists(tableName).update.run

    val columnsWithTypes = params.fields
      .map(_._1)
      .foldLeft(List.empty[String]) {
        case (acc, name) =>
          val uName = uniqueName(acc.toSet, name.take(63))
          uName :: acc
      }
      .reverse
      .zip(mapColumnTypes(params))

    val create = createContractTable(tableName, columnsWithTypes).update.run
    val setComment = setTableComment(
      tableName,
      s"Template IDs:\n${idAsComment(templateId)}"
    ).update.run

    val createIndexT = createIndex(tableName, NonEmptyList("_transaction_id")).update.run
    val createIndexA =
      createIndex(tableName, NonEmptyList("_archived_by_transaction_id")).update.run

    drop *> create *> setComment *> createIndexA *> createIndexT.void
  }

  private def mapSQLType(iType: FullyAppliedType): String = iType match {
    case TypePrim(typ, _) =>
      typ match {
        case iface.PrimTypeParty => "TEXT"
        case iface.PrimTypeList => "JSONB"
        case iface.PrimTypeContractId => "TEXT"
        case iface.PrimTypeTimestamp => "TIMESTAMP"
        case iface.PrimTypeDecimal => "NUMERIC(38, 10)"
        case iface.PrimTypeBool => "BOOLEAN"
        case iface.PrimTypeUnit => "SMALLINT"
        case iface.PrimTypeInt64 => "BIGINT"
        case iface.PrimTypeText => "TEXT"
        case iface.PrimTypeDate => "DATE"
        case iface.PrimTypeOptional => "JSONB"
        case iface.PrimTypeMap => "JSONB"
      }
    case TypeCon(_, _) => "JSONB"
  }

  private def mapColumnTypes(params: iface.Record[(String, iface.Type)]): List[String] = {
    params.fields.toList.map(_._2.fat).map {
      case TypePrim(iface.PrimTypeOptional, typeArg :: _) => mapSQLType(typeArg) + " NULL"
      case other => mapSQLType(other) + " NOT NULL"
    }
  }

  private def uniqueName(takenNames: Set[String], baseName: String): String = {
    @tailrec
    def postfixed(i: Int): String = {
      // truncate it back so it won't be longer than 63 (PostgreSQL id length limit) with postfix `__(n+)`
      val spaceLeft = (60 - Math.floor(Math.log10(i.toDouble))).toInt
      val current = s"${baseName.take(spaceLeft)}__${i}"
      if (takenNames.contains(current)) {
        postfixed(i + 1)
      } else {
        current
      }
    }
    if (takenNames.contains(baseName)) {
      postfixed(1)
    } else {
      baseName
    }
  }

  private def idAsComment(id: Identifier): String = s"${id.name}@${id.packageId}"
}
