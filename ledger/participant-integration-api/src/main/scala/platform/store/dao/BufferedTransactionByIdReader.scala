// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.lf.data.Ref.Party
import com.daml.logging.LoggingContext
import com.daml.platform.store.cache.InMemoryFanoutBuffer
import com.daml.platform.store.interfaces.TransactionLogUpdate

import scala.concurrent.Future

class BufferedTransactionByIdReader[API_RESPONSE](
    inMemoryFanout: InMemoryFanoutBuffer,
    fetchFromPersistence: String => Set[Party] => LoggingContext => Future[Option[API_RESPONSE]],
    toApiResponse: TransactionLogUpdate.TransactionAccepted => Set[Party] => Future[
      Option[API_RESPONSE]
    ],
) {

  def fetch(transactionId: String, requestingParties: Set[Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[API_RESPONSE]] =
    inMemoryFanout.lookup(transactionId) match {
      case Some(value) => toApiResponse(value)(requestingParties)
      case None =>
        fetchFromPersistence(transactionId)(requestingParties)(loggingContext)
    }
}
