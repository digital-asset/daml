// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}

/** Bundles all data extracted from ACTs together. */
final case class TransactionWithMeta(
    transaction: GenTransaction[String, AbsoluteContractId, VersionedValue[AbsoluteContractId]],
    meta: TransactionMeta)
