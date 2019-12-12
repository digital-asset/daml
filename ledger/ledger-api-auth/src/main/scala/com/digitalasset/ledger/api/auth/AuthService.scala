// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth

import java.util.concurrent.CompletionStage

/** An interface for authorizing the ledger API access to a participant.
  *
  * The AuthService is responsible for converting request metadata (such as
  * the HTTP headers) into a set of [[Claims]].
  * These claims are then used by the ledger API server to check whether the
  * request is authorized.
  *
  * For example, a participant could:
  * - Ask all ledger API users to attach an `Authorization` header
  *   with a JWT token as the header value.
  * - Implement `decodeMetadata()` such that it reads the JWT token
  *   from the corresponding HTTP header, validates the token,
  *   and converts the token payload to [[Claims]].
  */
trait AuthService {

  /** Converts gRPC request metadata into a set of [[Claims]].
    *
    *  @param headers All HTTP headers attached to the request.
    *
    */
  def decodeMetadata(headers: io.grpc.Metadata): CompletionStage[Claims]
}
