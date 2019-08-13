// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

/** Bundles all data extracted from ACTs together. */
final case class TransactionWithMeta(
    transaction: GenTransaction.WithTxValue[Ref.LedgerString, AbsoluteContractId],
    meta: TransactionMeta)
