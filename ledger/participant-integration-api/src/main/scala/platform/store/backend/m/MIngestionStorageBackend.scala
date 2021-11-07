// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.{DbDto, IngestionStorageBackend, ParameterStorageBackend}
import com.daml.platform.store.interning.StringInterning
import DbDtoUtils._

import scala.collection.mutable.ArrayBuffer

object MIngestionStorageBackend extends IngestionStorageBackend[Vector[DbDto]] {
  override def batch(dbDtos: Vector[DbDto], stringInterning: StringInterning): Vector[DbDto] =
    dbDtos

  override def insertBatch(connection: Connection, batch: Vector[DbDto]): Unit = {
    val events = ArrayBuffer.empty[DbDto]
    val configurations = ArrayBuffer.empty[DbDto.ConfigurationEntry]
    val parties = ArrayBuffer.empty[DbDto.PartyEntry]
    val packages = ArrayBuffer.empty[(String, DbDto.Package)]
    val packageEntries = ArrayBuffer.empty[DbDto.PackageEntry]
    val stringInternings = ArrayBuffer.empty[DbDto.StringInterningDto]
    val commandCompletions = ArrayBuffer.empty[DbDto.CommandCompletion]
    val commandDeduplications = ArrayBuffer.empty[String]
    batch.foreach {
      case e: DbDto.EventCreate => events.addOne(e)
      case e: DbDto.EventDivulgence => events.addOne(e)
      case e: DbDto.EventExercise => events.addOne(e)
      case e: DbDto.ConfigurationEntry => configurations.addOne(e)
      case e: DbDto.PartyEntry => parties.addOne(e)
      case e: DbDto.Package => packages.addOne(e.package_id -> e)
      case e: DbDto.PackageEntry => packageEntries.addOne(e)
      case e: DbDto.StringInterningDto => stringInternings.addOne(e)
      case e: DbDto.CommandCompletion => commandCompletions.addOne(e)
      case e: DbDto.CommandDeduplication => commandDeduplications.addOne(e.deduplication_key)
      case _: DbDto.CreateFilter => ()
    }
    def updateIndex[T](
        base: Map[String, Vector[T]],
        newElem: (String, T),
    ): Map[String, Vector[T]] =
      base.get(newElem._1) match {
        case Some(dtos) => base + newElem.copy(_2 = dtos :+ newElem._2)
        case None => base + newElem.copy(_2 = Vector(newElem._2))
      }

    val store = MStore(connection)
    store.synchronized {
      val base = store.mData
      val newMData = MData(
        events = base.events ++ events,
        contractIdIndex = events.iterator
          .collect {
            case dto: DbDto.EventCreate => dto.contract_id -> dto
            case dto: DbDto.EventExercise => dto.contract_id -> dto
            case dto: DbDto.EventDivulgence => dto.contract_id -> dto
          }
          .foldLeft(base.contractIdIndex)(updateIndex),
        contractKeyIndex = events.iterator
          .flatMap {
            case dto: DbDto.EventCreate => dto.create_key_hash.iterator.map(_ -> dto)
            case _ => Iterator.empty
          }
          .foldLeft(base.contractKeyIndex)(updateIndex),
        transactionIdIndex = events.iterator
          .flatMap {
            case dto: DbDto.EventCreate => dto.transaction_id.iterator.map(_ -> dto)
            case dto: DbDto.EventExercise => dto.transaction_id.iterator.map(_ -> dto)
            case _ => Iterator.empty
          }
          .foldLeft(base.transactionIdIndex)(updateIndex),
        configurations = base.configurations ++ configurations,
        partyEntries = base.partyEntries ++ parties,
        partyIndex = parties.iterator
          .collect {
            case p if p.typ == "accept" => p.party.get -> p
          }
          .foldLeft(base.partyIndex)(updateIndex),
        stringInternings = base.stringInternings ++ stringInternings,
        commandCompletions = base.commandCompletions ++ commandCompletions,
        packageEntries = base.packageEntries ++ packageEntries,
        packages = base.packages ++ (packages.filterNot(p => base.packages.contains(p._1))),
      )
      MStore(connection).mData = newMData
      if (commandDeduplications.nonEmpty) {
        val commandSubmissions = MStore(connection).commandSubmissions
        commandSubmissions.synchronized {
          commandSubmissions --= commandDeduplications
        }
      }
      ()
    }
  }

  override def deletePartiallyIngestedData(
      ledgerEnd: Option[ParameterStorageBackend.LedgerEnd]
  )(connection: Connection): Unit =
    ledgerEnd.foreach { ledgerEnd =>
      val hexLedgerEnd = ledgerEnd.lastOffset.toHexString
      val mStore = MStore(connection)
      def filterEventIndex(index: Map[String, Vector[DbDto]]): Map[String, Vector[DbDto]] =
        index.iterator.flatMap { case (k, v) =>
          val newV = v.takeWhile(_.eventSeqId <= ledgerEnd.lastEventSeqId)
          if (newV.isEmpty) Iterator.empty
          else Iterator(k -> newV)
        }.toMap
      mStore.synchronized {
        val mData = mStore.mData
        mStore.mData = MData(
          events = mData.events.take(ledgerEnd.lastEventSeqId.toInt),
          contractIdIndex = filterEventIndex(mData.contractIdIndex),
          contractKeyIndex = filterEventIndex(mData.contractKeyIndex),
          transactionIdIndex = filterEventIndex(mData.transactionIdIndex),
          configurations = mData.configurations.takeWhile(_.ledger_offset <= hexLedgerEnd),
          partyEntries = mData.partyEntries.takeWhile(_.ledger_offset <= hexLedgerEnd),
          partyIndex = mData.partyIndex.iterator.flatMap { case (k, v) =>
            val newV = v.takeWhile(_.ledger_offset <= ledgerEnd.lastOffset.toHexString)
            if (newV.isEmpty) Iterator.empty
            else Iterator(k -> newV)
          }.toMap,
          stringInternings = mData.stringInternings,
          commandCompletions =
            mData.commandCompletions.takeWhile(_.completion_offset <= hexLedgerEnd),
          packageEntries = mData.packageEntries.takeWhile(_.ledger_offset <= hexLedgerEnd),
          packages = mData.packages.filter(_._2.ledger_offset <= hexLedgerEnd),
        )
      }
    }
}
