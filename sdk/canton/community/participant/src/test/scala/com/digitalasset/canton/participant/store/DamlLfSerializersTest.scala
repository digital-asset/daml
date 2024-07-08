// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.protocol.WellFormedTransaction.WithSuffixes
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, WellFormedTransaction}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class DamlLfSerializersTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  "transaction serialization and deserialization" when {

    val etf = new ExampleTransactionFactory()()
    forEvery(etf.standardHappyCases) { transaction =>
      transaction.toString must {
        "have the same value after serialization and deserialization" in {
          val wellFormedTransaction = transaction.wellFormedSuffixedTransaction
          val proto = valueOrFail(
            DamlLfSerializers.serializeTransaction(wellFormedTransaction.unwrap)
          )("failed to serialize transaction")
          val decodedVersionedTx = DamlLfSerializers.deserializeTransaction(proto).value
          val wellFormedDecoded =
            WellFormedTransaction.normalizeAndAssert(
              decodedVersionedTx,
              wellFormedTransaction.metadata,
              WithSuffixes,
            )

          wellFormedDecoded shouldEqual wellFormedTransaction
        }
      }
    }
  }
}
