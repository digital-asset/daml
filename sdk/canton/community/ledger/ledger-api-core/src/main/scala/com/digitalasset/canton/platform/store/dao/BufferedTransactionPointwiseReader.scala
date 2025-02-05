// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.dao.BufferedTransactionByIdReader.{
  FetchTransactionPointwiseFromPersistence,
  ToApiResponse,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.TransactionAccepted

import scala.concurrent.Future

/** Generic class that helps serving Ledger API point-wise lookups
  *  (UpdateService.{GetTransactionById, GetTransactionTreeById,
  *  GetTransactionByOffset, GetTransactionTreeByOffset})
  *  from either the in-memory fan-out buffer or from persistence.
  *
  * @param fetchFromPersistence Fetch a transaction by offset or id from persistence.
  * @param toApiResponse Convert a [[com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.TransactionAccepted]] to a specific API response
  *                      while also filtering for visibility.
  * @tparam QUERY_PARAM_TYPE The query parameter type.
  * @tparam API_RESPONSE The Ledger API response type.
  */
class BufferedTransactionPointwiseReader[QUERY_PARAM_TYPE, API_RESPONSE](
    fetchFromPersistence: FetchTransactionPointwiseFromPersistence[QUERY_PARAM_TYPE, API_RESPONSE],
    fetchFromBuffer: QUERY_PARAM_TYPE => Option[TransactionAccepted],
    toApiResponse: ToApiResponse[QUERY_PARAM_TYPE, API_RESPONSE],
) {

  /** Serves processed and filtered transaction from the buffer by the query parameter,
    * with fallback to a persistence fetch if the transaction is not anymore in the buffer
    * (i.e. it was evicted)
    *
    * @param queryParam The query parameter.
    * @param loggingContext The logging context
    * @return A future wrapping the API response if found.
    */
  def fetch(queryParam: QUERY_PARAM_TYPE)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[API_RESPONSE]] =
    fetchFromBuffer(queryParam) match {
      case Some(value) => toApiResponse(value, queryParam, loggingContext)
      case None =>
        fetchFromPersistence(queryParam, loggingContext)
    }
}

object BufferedTransactionByIdReader {
  trait FetchTransactionPointwiseFromPersistence[QUERY_PARAM_TYPE, API_RESPONSE] {
    def apply(
        queryParam: QUERY_PARAM_TYPE,
        loggingContext: LoggingContextWithTrace,
    ): Future[Option[API_RESPONSE]]
  }

  trait ToApiResponse[QUERY_PARAM_TYPE, API_RESPONSE] {
    def apply(
        transactionAccepted: TransactionAccepted,
        queryParam: QUERY_PARAM_TYPE,
        loggingContext: LoggingContextWithTrace,
    ): Future[Option[API_RESPONSE]]
  }
}
