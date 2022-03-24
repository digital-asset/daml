// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    val transactionMeterings = ArrayBuffer.empty[DbDto.TransactionMetering]
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
      case _: DbDto.CreateFilter => ()
      case e: DbDto.TransactionMetering => transactionMeterings.addOne(e)
    }
    def updateIndex[T](
        base: Map[String, Vector[T]],
        newElem: (String, T),
    ): Map[String, Vector[T]] =
      base.get(newElem._1) match {
        case Some(dtos) => base + newElem.copy(_2 = dtos :+ newElem._2)
        case None => base + newElem.copy(_2 = Vector(newElem._2))
      }

    MStore.update(connection) { mStore =>
      mStore.copy(
        events = mStore.events ++ events,
        contractIdIndex = events.iterator
          .collect {
            case dto: DbDto.EventCreate => dto.contract_id -> dto
            case dto: DbDto.EventExercise => dto.contract_id -> dto
            case dto: DbDto.EventDivulgence => dto.contract_id -> dto
          }
          .foldLeft(mStore.contractIdIndex)(updateIndex),
        contractKeyIndex = events.iterator
          .flatMap {
            case dto: DbDto.EventCreate => dto.create_key_hash.iterator.map(_ -> dto)
            case _ => Iterator.empty
          }
          .foldLeft(mStore.contractKeyIndex)(updateIndex),
        transactionIdIndex = events.iterator
          .flatMap {
            case dto: DbDto.EventCreate => dto.transaction_id.iterator.map(_ -> dto)
            case dto: DbDto.EventExercise => dto.transaction_id.iterator.map(_ -> dto)
            case _ => Iterator.empty
          }
          .foldLeft(mStore.transactionIdIndex)(updateIndex),
        configurations = mStore.configurations ++ configurations,
        partyEntries = mStore.partyEntries ++ parties,
        partyIndex = parties.iterator
          .collect {
            case p if p.typ == "accept" => p.party.get -> p
          }
          .foldLeft(mStore.partyIndex)(updateIndex),
        stringInternings = mStore.stringInternings ++ stringInternings,
        commandCompletions = mStore.commandCompletions ++ commandCompletions,
        packageEntries = mStore.packageEntries ++ packageEntries,
        packages = mStore.packages ++ (packages.filterNot(p => mStore.packages.contains(p._1))),
        transactionMetering = mStore.transactionMetering ++ transactionMeterings,
      )
    }
  }

  override def deletePartiallyIngestedData(
      ledgerEnd: ParameterStorageBackend.LedgerEnd
  )(connection: Connection): Unit = MStore.update(connection) { mStore =>
    val hexLedgerEnd = ledgerEnd.lastOffset.toHexString

    def filterEventIndex(index: Map[String, Vector[DbDto]]): Map[String, Vector[DbDto]] =
      index.iterator.flatMap { case (k, v) =>
        val newV = v.takeWhile(_.eventSeqId <= ledgerEnd.lastEventSeqId)
        if (newV.isEmpty) Iterator.empty
        else Iterator(k -> newV)
      }.toMap

    mStore.copy(
      events = mStore.events.take(ledgerEnd.lastEventSeqId.toInt),
      contractIdIndex = filterEventIndex(mStore.contractIdIndex),
      contractKeyIndex = filterEventIndex(mStore.contractKeyIndex),
      transactionIdIndex = filterEventIndex(mStore.transactionIdIndex),
      configurations = mStore.configurations.takeWhile(_.ledger_offset <= hexLedgerEnd),
      partyEntries = mStore.partyEntries.takeWhile(_.ledger_offset <= hexLedgerEnd),
      partyIndex = mStore.partyIndex.iterator.flatMap { case (k, v) =>
        val newV = v.takeWhile(_.ledger_offset <= ledgerEnd.lastOffset.toHexString)
        if (newV.isEmpty) Iterator.empty
        else Iterator(k -> newV)
      }.toMap,
      stringInternings = mStore.stringInternings,
      commandCompletions = mStore.commandCompletions.takeWhile(_.completion_offset <= hexLedgerEnd),
      packageEntries = mStore.packageEntries.takeWhile(_.ledger_offset <= hexLedgerEnd),
      packages = mStore.packages.filter(_._2.ledger_offset <= hexLedgerEnd),
      transactionMetering = mStore.transactionMetering.takeWhile(_.ledger_offset <= hexLedgerEnd),
    )
  }
}
