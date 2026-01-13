// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.LfVersionedTransaction
import com.digitalasset.canton.util.Mutex

import scala.collection.mutable

// WARNING this is only intended to used by testing
class OnlyForTestingTransactionInMemoryStore(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  private val store: mutable.Map[String, LfVersionedTransaction] = mutable.Map()
  private val lock = new Mutex()

  def put(updateId: String, lfVersionedTransaction: LfVersionedTransaction): Unit =
    (
      lock.exclusive(
        // Prevent massive accumulation, and also WARN heavily if potential abuse is detected
        if (store.sizeIs > 100) {
          noTracingLogger.warn(
            "OnlyForTestingTransactionInMemoryStore is being used, and accumulated 100 transactions. Please turn off testing configuration only-for-testing-enable-in-memory-transaction-store."
          )
        } else {
          discard(
            store += updateId -> lfVersionedTransaction
          )
        }
      )
    )

  def get(updateId: String): Option[LfVersionedTransaction] =
    (
      lock.exclusive(
        store.get(updateId)
      )
    )

}
