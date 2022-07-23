// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.platform.store.cache.InMemoryFanoutBuffer
import com.daml.platform.store.dao.BufferedTransactionByIdReader.{
  FetchTransactionByIdFromPersistence,
  ToApiResponse,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate.TransactionAccepted

import scala.concurrent.Future

class BufferedTransactionByIdReader[API_RESPONSE](
    inMemoryFanout: InMemoryFanoutBuffer,
    fetchFromPersistence: FetchTransactionByIdFromPersistence[API_RESPONSE],
    toApiResponse: ToApiResponse[API_RESPONSE],
) {

  def fetch(transactionId: String, requestingParties: Set[Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[API_RESPONSE]] =
    inMemoryFanout.lookup(transactionId) match {
      case Some(value) => toApiResponse(value, requestingParties, loggingContext)
      case None =>
        fetchFromPersistence(transactionId, requestingParties, loggingContext)
    }
}

object BufferedTransactionByIdReader {
  trait FetchTransactionByIdFromPersistence[API_RESPONSE] {
    def apply(
        transactionId: String,
        requestingParties: Set[Party],
        loggingContext: LoggingContext,
    ): Future[Option[API_RESPONSE]]
  }

  trait ToApiResponse[API_RESPONSE] {
    def apply(
        transactionAccepted: TransactionAccepted,
        requestingParties: Set[Party],
        loggingContext: LoggingContext,
    ): Future[Option[API_RESPONSE]]
  }
}
