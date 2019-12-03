// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** Information allowing tracing of the requests across different distributed components
  *
  * TracingInfo is used in all the ReadService and WriteService calls
  *
  * @param correlationId The unique identifier of the request, assigned by the ledger api server
  *                      upon receipt of a client command
  *
  */
final case class TracingInfo(correlationId: String)
