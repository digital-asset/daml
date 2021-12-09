// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

sealed trait StreamResult extends Product with Serializable

object StreamResult {
  final case object Ok extends StreamResult
  final case object ObjectivesViolated extends StreamResult
}
