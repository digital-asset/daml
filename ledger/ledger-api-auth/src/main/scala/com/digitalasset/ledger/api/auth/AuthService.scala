// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

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

  /**
    * Return empty [[Claims]] to reject requests with a UNAUTHENTICATED error status.
    * Return [[Claims]] with only a single [[ClaimPublic]] claim to reject all non-public requests with a PERMISSION_DENIED status.
    * Return a failed future to reject requests with an INTERNAL error status.
    */
  def decodeMetadata(headers: io.grpc.Metadata): CompletionStage[Claims]
}
