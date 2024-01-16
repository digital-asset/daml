// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.util.concurrent.CompletionStage

import io.grpc.Metadata

/** An interface for authorizing the ledger API access to a participant.
  *
  * The AuthService is responsible for converting request metadata (such as
  * the HTTP headers) into a [[ClaimSet]].
  * These claims are then used by the ledger API server to check whether the
  * request is authorized.
  *
  * - The authorization information MUST be specified in the `Authorization` header.
  * - The value of the `Authorization` header MUST start with `Bearer `
  *   (notice the trailing space of the prefix).
  * - An [[AuthService]] implementation MAY use other headers when converting metadata
  *   to claims.
  *
  * For example, a participant could:
  * - Ask all ledger API users to attach an `Authorization` header
  *   with a JWT token as the header value.
  * - Implement `decodeMetadata()` such that it reads the JWT token
  *   from the corresponding HTTP header, validates the token,
  *   and converts the token payload to [[ClaimSet]].
  */
trait AuthService {

  /** Return empty [[ClaimSet.Unauthenticated]] to reject requests with a UNAUTHENTICATED error status.
    * Return [[ClaimSet.Claims]] with only a single [[ClaimPublic]] claim to reject all non-public requests with a PERMISSION_DENIED status.
    * Return a failed future to reject requests with an INTERNAL error status.
    */
  def decodeMetadata(headers: io.grpc.Metadata): CompletionStage[ClaimSet]

}

object AuthService {

  /** The [[Metadata.Key]] to use for looking up the `Authorization` header in the
    * request metadata.
    */
  val AUTHORIZATION_KEY: Metadata.Key[String] =
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
}
