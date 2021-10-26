// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

final case class ContractSetDescriptor(
    numberOfInstances: Int,
    numberOfObservers: Int,
    instanceDistribution: List[ContractSetDescriptor.ContractDescription],
)

object ContractSetDescriptor {
  final case class ContractDescription(
      template: String,
      weight: Int,
      payloadSizeBytes: Int,
  )
}
