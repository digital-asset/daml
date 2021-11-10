// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

final case class WorkflowDescriptor(
    submission: Option[SubmissionDescriptor],
    streams: List[StreamDescriptor] = List.empty,
)

final case class SubmissionDescriptor(
    numberOfInstances: Int,
    numberOfObservers: Int,
    uniqueParties: Boolean,
    instanceDistribution: List[SubmissionDescriptor.ContractDescription],
)

object SubmissionDescriptor {
  final case class ContractDescription(
      template: String,
      weight: Int,
      payloadSizeBytes: Int,
      archiveChance: Double,
  )
}

final case class StreamDescriptor(
    streamType: StreamDescriptor.StreamType,
    name: String,
    filters: List[StreamDescriptor.PartyFilter],
)

object StreamDescriptor {
  sealed trait StreamType
  object StreamType {
    case object Transactions extends StreamType
    case object TransactionTrees extends StreamType
    case object ActiveContracts extends StreamType
  }

  final case class PartyFilter(party: String, templates: List[String])
}
