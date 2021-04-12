// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v1
import com.softwaremill.diffx.{Derived, Diff}
import io.grpc.health.v1.health.HealthCheckResponse.ServingStatus

object DiffInstances {
  // NOTE (MK) Ideally, we would use semi-automatic derivation here but
  // that fails due to https://github.com/softwaremill/diffx/issues/237
  import com.softwaremill.diffx.generic.auto._
  implicit val diffIdentifier: Derived[Diff[v1.value.Identifier]] =
    Derived[Diff[v1.value.Identifier]]

  implicit val diffValue: Derived[Diff[v1.value.Value]] = Derived[Diff[v1.value.Value]]
  implicit val diffRecordField: Derived[Diff[v1.value.RecordField]] =
    Derived[Diff[v1.value.RecordField]]
  implicit val diffRecord: Derived[Diff[v1.value.Record]] = Derived[Diff[v1.value.Record]]

  implicit val diffTransaction: Derived[Diff[v1.transaction.Transaction]] =
    Derived[Diff[v1.transaction.Transaction]]
  implicit val diffTransactionTree: Derived[Diff[v1.transaction.TransactionTree]] =
    Derived[Diff[v1.transaction.TransactionTree]]

  implicit val diffServingStatus: Derived[Diff[ServingStatus]] = Derived[Diff[ServingStatus]]
}
