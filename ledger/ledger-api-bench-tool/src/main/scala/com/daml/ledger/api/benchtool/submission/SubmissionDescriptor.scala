// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

final case class WorkflowDescriptor(
    submission: SubmissionDescriptor
)

final case class SubmissionDescriptor(
    numberOfInstances: Int,
    numberOfObservers: Int,
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
