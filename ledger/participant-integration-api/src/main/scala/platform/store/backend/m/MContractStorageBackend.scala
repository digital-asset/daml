// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.store.appendonlydao.events.{ContractId, Key}
import com.daml.platform.store.backend.{ContractStorageBackend, DbDto}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.{KeyAssigned, KeyUnassigned}

import scala.util.{Failure, Success, Try}

class MContractStorageBackend(ledgerEndCache: LedgerEndCache) extends ContractStorageBackend {

  override def contractKeyGlobally(key: Key)(connection: Connection): Option[ContractId] = {
    val mData = MStore(connection).mData
    val ledgerEndSequentialId = ledgerEndCache()._2
    mData.contractKeyIndex
      .getOrElse(key.hash.bytes.toHexString, Vector.empty)
      .reverseIterator
      .collect { case c: DbDto.EventCreate => c }
      .find(_.event_sequential_id <= ledgerEndSequentialId)
      .filterNot(mData.archived(ledgerEndSequentialId))
      .map(create => ContractId.assertFromString(create.contract_id))
  }

  override def maximumLedgerTime(
      ids: Set[ContractId]
  )(connection: Connection): Try[Option[Time.Timestamp]] = {
    val lastEventSequentialId = ledgerEndCache()._2
    val mData = MStore(connection).mData

    def emptyContractIds: Throwable =
      new IllegalArgumentException(
        "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
      )

    if (ids.isEmpty) {
      Failure(emptyContractIds)
    } else {
      def lookup(id: ContractId): Option[Option[Timestamp]] = {
        mData.contractIdIndex
          .get(id.coid)
          .filter(_.nonEmpty)
          .filterNot(_.exists(mData.archived(lastEventSequentialId)))
          .flatMap(eventsForContractId =>
            eventsForContractId
              .collectFirst { case c: DbDto.EventCreate =>
                c.ledger_effective_time.map(Time.Timestamp(_))
              }
              .orElse(
                eventsForContractId.collectFirst { case _: DbDto.EventDivulgence =>
                  None
                }
              )
          )
      }

      val queriedIds: List[(ContractId, Option[Option[Timestamp]])] = ids.toList
        .map(id => id -> lookup(id))
      val foundLedgerEffectiveTimes: List[Option[Timestamp]] = queriedIds
        .collect { case (_, Some(found)) =>
          found
        }
      if (foundLedgerEffectiveTimes.size != ids.size) {
        val missingIds = queriedIds.collect { case (missingId, None) =>
          missingId
        }
        Failure(MissingContracts(missingIds.toSet))
      } else Success(foundLedgerEffectiveTimes.max)
    }
  }

  override def keyState(key: Key, validAt: Long)(
      connection: Connection
  ): LedgerDaoContractsReader.KeyState = {
    val mData = MStore(connection).mData
    mData.contractKeyIndex
      .getOrElse(key.hash.bytes.toHexString, Vector.empty)
      .reverseIterator
      .collect { case c: DbDto.EventCreate => c }
      .find(_.event_sequential_id <= validAt)
      .filterNot(mData.archived(validAt))
      .map(create =>
        KeyAssigned(
          contractId = ContractId.assertFromString(create.contract_id),
          stakeholders = create.flat_event_witnesses.map(Ref.Party.assertFromString),
        )
      )
      .getOrElse(KeyUnassigned)
  }

  override def contractState(contractId: ContractId, before: Long)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContractState] = {
    val mData = MStore(connection).mData
    mData.contractIdIndex
      .getOrElse(contractId.coid, Vector.empty)
      .reverseIterator
      .collectFirst {
        case c: DbDto.EventCreate if c.event_sequential_id <= before =>
          ContractStorageBackend.RawContractState(
            templateId = c.template_id,
            flatEventWitnesses = c.flat_event_witnesses.map(Ref.Party.assertFromString),
            createArgument = c.create_argument,
            createArgumentCompression = c.create_argument_compression,
            eventKind = 10,
            ledgerEffectiveTime = c.ledger_effective_time.map(Time.Timestamp(_)),
          )
        case c: DbDto.EventExercise if c.consuming && c.event_sequential_id <= before =>
          ContractStorageBackend.RawContractState(
            templateId = c.template_id,
            flatEventWitnesses = c.flat_event_witnesses.map(Ref.Party.assertFromString),
            createArgument = None,
            createArgumentCompression = None,
            eventKind = 20,
            ledgerEffectiveTime = c.ledger_effective_time.map(Time.Timestamp(_)),
          )
      }
  }

  override def activeContractWithArgument(readers: Set[Party], contractId: ContractId)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContract] = {
    val mData = MStore(connection).mData
    val ledgerEndSequentialId = ledgerEndCache()._2
    val stringReaders = readers.map(_.toString)
    mData.contractIdIndex
      .get(contractId.coid)
      .filter(_.nonEmpty)
      .filterNot(_.exists {
        case archival: DbDto.EventExercise if archival.consuming =>
          archival.event_sequential_id <= ledgerEndSequentialId &&
            archival.tree_event_witnesses.exists(stringReaders)

        case _ => false
      })
      .flatMap { dtos =>
        dtos
          .collectFirst {
            case create: DbDto.EventCreate
                if create.event_sequential_id <= ledgerEndSequentialId &&
                  create.tree_event_witnesses.exists(stringReaders) =>
              new ContractStorageBackend.RawContract(
                templateId = create.template_id.get,
                createArgument = create.create_argument.get,
                createArgumentCompression = create.create_argument_compression,
              )
          }
          .orElse {
            dtos
              .collectFirst {
                case divulged: DbDto.EventDivulgence
                    if divulged.event_sequential_id <= ledgerEndSequentialId &&
                      divulged.tree_event_witnesses.exists(stringReaders) =>
                  divulged
              }
              .flatMap { divulged =>
                (divulged.template_id, divulged.create_argument) match {
                  case (Some(templateId), Some(createArgument)) =>
                    Some(
                      new ContractStorageBackend.RawContract(
                        templateId = templateId,
                        createArgument = createArgument,
                        createArgumentCompression = divulged.create_argument_compression,
                      )
                    )

                  case _ =>
                    // try to get template id and create argument from an unrestricted create before ledger end
                    dtos.collectFirst {
                      case create: DbDto.EventCreate
                          if create.event_sequential_id <= ledgerEndSequentialId =>
                        new ContractStorageBackend.RawContract(
                          templateId = create.template_id.get,
                          createArgument = create.create_argument.get,
                          createArgumentCompression = create.create_argument_compression,
                        )
                    }
                }
              }
          }
      }
  }

  override def activeContractWithoutArgument(readers: Set[Party], contractId: ContractId)(
      connection: Connection
  ): Option[String] = activeContractWithArgument(readers, contractId)(connection).map(_.templateId)

  override def contractKey(readers: Set[Party], key: Key)(
      connection: Connection
  ): Option[ContractId] = {
    val mData = MStore(connection).mData
    val ledgerEndSequentialId = ledgerEndCache()._2
    val stringReaders = readers.map(_.toString)
    mData.contractKeyIndex
      .getOrElse(key.hash.bytes.toHexString, Vector.empty)
      .reverseIterator
      .collect { case c: DbDto.EventCreate => c }
      .find(_.event_sequential_id <= ledgerEndSequentialId)
      .filter(_.flat_event_witnesses.exists(stringReaders))
      .filterNot(mData.archived(ledgerEndSequentialId))
      .map(create => ContractId.assertFromString(create.contract_id))
  }

  override def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[ContractStorageBackend.RawContractStateEvent] =
    MStore(connection).mData
      .eventRange(startExclusive, endInclusive)
      .collect {
        case c: DbDto.EventCreate =>
          ContractStorageBackend.RawContractStateEvent(
            eventKind = 10,
            contractId = ContractId.assertFromString(c.contract_id),
            templateId = c.template_id.map(Ref.Identifier.assertFromString),
            ledgerEffectiveTime = c.ledger_effective_time.map(Time.Timestamp(_)),
            createKeyValue = c.create_key_value,
            createKeyCompression = c.create_key_value_compression,
            createArgument = c.create_argument,
            createArgumentCompression = c.create_argument_compression,
            flatEventWitnesses = c.flat_event_witnesses.map(Ref.Party.assertFromString),
            eventSequentialId = c.event_sequential_id,
            offset = Offset.fromHexString(Ref.HexString.assertFromString(c.event_offset.get)),
          )
        case c: DbDto.EventExercise if c.consuming =>
          ContractStorageBackend.RawContractStateEvent(
            eventKind = 20,
            contractId = ContractId.assertFromString(c.contract_id),
            templateId = c.template_id.map(Ref.Identifier.assertFromString),
            ledgerEffectiveTime = c.ledger_effective_time.map(Time.Timestamp(_)),
            createKeyValue = c.create_key_value,
            createKeyCompression = c.create_key_value_compression,
            createArgument = None,
            createArgumentCompression = None,
            flatEventWitnesses = c.flat_event_witnesses.map(Ref.Party.assertFromString),
            eventSequentialId = c.event_sequential_id,
            offset = Offset.fromHexString(Ref.HexString.assertFromString(c.event_offset.get)),
          )
      }
      .toVector
}
