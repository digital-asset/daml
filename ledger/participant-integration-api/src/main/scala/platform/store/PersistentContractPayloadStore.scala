// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.io.ByteArrayInputStream

import com.daml.ledger.participant.state.v2.ContractPayloadStore
import com.daml.lf.value.Value
import com.daml.lf.value.Value.VersionedContractInstance
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.PersistentContractPayloadStore.localLoggingContext
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.appendonlydao.events.{Contract => LfContract}
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.serialization.ValueSerializer

import scala.concurrent.{ExecutionContext, Future}

class PersistentContractPayloadStore(
    dbDispatcher: DbDispatcher,
    contractStorageBackend: ContractStorageBackend,
    metrics: Metrics,
)(implicit
    deserializationExecutionContext: ExecutionContext
) extends ContractPayloadStore {

  // TODO: global queueing and re-batching might help here
  override def loadContractPayloads(
      contractIds: Set[Value.ContractId]
  ): Future[Map[Value.ContractId, VersionedContractInstance]] = {
    dbDispatcher
      .executeSql(metrics.daml.index.db.getContractPayloads)(
        contractStorageBackend.contractPayloadBatch(contractIds)
      )(localLoggingContext)
      .flatMap { rawContractPayloads =>
        Future {
          rawContractPayloads.map { // TODO suboptimal: sequential deserializing
            case (id, (payload, templateId)) =>
              val deserialized = ValueSerializer.deserializeValue(new ByteArrayInputStream(payload))
              val contract = LfContract(
                template = templateId,
                arg = deserialized,
                agreementText = "",
              )
              id -> contract
          }
        }
      }
  }
}

object PersistentContractPayloadStore {
  private val localLoggingContext = LoggingContext.empty
}
