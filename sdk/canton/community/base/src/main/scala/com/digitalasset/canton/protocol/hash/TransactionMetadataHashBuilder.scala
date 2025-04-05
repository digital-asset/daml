// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value.ContractId

import java.util.UUID
import scala.collection.immutable.{SortedMap, SortedSet}

object TransactionMetadataHashBuilder {
  final case class MetadataV1(
      actAs: SortedSet[Ref.Party],
      commandId: Ref.CommandId,
      transactionUUID: UUID,
      mediatorGroup: Int,
      synchronizerId: String,
      ledgerEffectiveTime: Option[Time.Timestamp],
      submissionTime: Time.Timestamp,
      disclosedContracts: SortedMap[ContractId, FatContractInstance],
  )

  /** Hashes Transaction Metadata using the V1 Hashing Scheme
    */
  @throws[NodeHashingError]
  def hashTransactionMetadataV1(
      metadata: TransactionMetadataHashBuilder.MetadataV1,
      hashTracer: HashTracer = HashTracer.NoOp,
  ): Hash =
    // Do not enforce node seed for create nodes here as we hash disclosed events which do not have a seed
    new NodeBuilderV1(
      HashPurpose.PreparedSubmission,
      hashTracer,
      enforceNodeSeedForCreateNodes = false,
    ).addPurpose
      .addMetadataEncodingVersion(1)
      .withContext("Act As Parties")(
        _.iterateOver(metadata.actAs.iterator, metadata.actAs.size)(_ add _)
      )
      .withContext("Command Id")(_.add(metadata.commandId))
      .withContext("Transaction UUID")(_.add(metadata.transactionUUID.toString))
      .withContext("Mediator Group")(_.add(metadata.mediatorGroup))
      .withContext("Synchronizer Id")(_.add(metadata.synchronizerId))
      .withContext("Ledger Effective Time")(
        _.addOptional(
          metadata.ledgerEffectiveTime.map(_.micros),
          builder => (value: Long) => builder.add(value),
        )
      )
      .withContext("Submission Time")(_.add(metadata.submissionTime.micros))
      .withContext("Disclosed Contracts")(
        _.iterateOver(metadata.disclosedContracts.valuesIterator, metadata.disclosedContracts.size)(
          (builder, fatInstance) =>
            builder
              .withContext("Created At")(_.add(fatInstance.createdAt.micros))
              .withContext("Create Contract")(builder =>
                builder.addHash(
                  builder.hashNode(
                    node = fatInstance.toCreateNode,
                    nodeSeed = Option.empty[LfHash],
                    nodes = Map.empty,
                    nodeSeeds = Map.empty,
                    hashTracer = hashTracer.subNodeTracer,
                  ),
                  "Disclosed Contract",
                )
              )
        )
      )
      .finish()
}
