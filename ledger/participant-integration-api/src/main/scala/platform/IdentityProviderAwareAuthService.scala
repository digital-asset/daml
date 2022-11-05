// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.jwt.{JwksVerifier, JwtTimestampLeeway}
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, ClaimSet}
import com.daml.ledger.api.domain.IdentityProviderConfig
import io.grpc.Metadata

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.concurrent.TrieMap

//TODO DPP-1299 Move to a package
class IdentityProviderAwareAuthService(
    defaultAuthService: AuthService,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
) extends AuthService {

  private val services = TrieMap[String, IdentityProviderAwareAuthService.IdpEntry]()

  def addService(identityProviderConfig: IdentityProviderConfig): Boolean = {
    val service = AuthServiceJWT(JwksVerifier(identityProviderConfig.jwksURL, jwtTimestampLeeway))
    val entry = IdentityProviderAwareAuthService.IdpEntry(
      identityProviderConfig.identityProviderId,
      identityProviderConfig.issuer,
      service,
    )
    services.put(entry.id, entry).isEmpty
  }

  def removeService(id: String): Boolean = {
    services.remove(id).isDefined
  }

  private val deny = CompletableFuture.completedFuture(ClaimSet.Unauthenticated: ClaimSet)

  private def claimCheck(
      entry: IdentityProviderAwareAuthService.IdpEntry,
      headers: Metadata,
  ): CompletionStage[ClaimSet] =
    entry.service.decodeMetadata(headers).thenApply {
      case ClaimSet.AuthenticatedUser(issuer, _, _, _) if !issuer.contains(entry.issuer) =>
        ClaimSet.Unauthenticated
      case otherwise => otherwise
    }

  private def claimCheck(
      prevClaims: ClaimSet,
      entry: IdentityProviderAwareAuthService.IdpEntry,
      headers: Metadata,
  ): CompletionStage[ClaimSet] = {
    if (prevClaims != ClaimSet.Unauthenticated)
      CompletableFuture.completedFuture(prevClaims)
    else
      claimCheck(entry, headers)
  }

  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] = {
    services.values.toSeq
      .foldLeft(deny) { case (acc, elem) =>
        acc.thenCompose(prevClaims => claimCheck(prevClaims, elem, headers))
      }
      .thenCompose { prevClaims =>
        if (prevClaims != ClaimSet.Unauthenticated)
          CompletableFuture.completedFuture(prevClaims)
        else {
          defaultAuthService.decodeMetadata(headers)
        }
      }
  }
}

object IdentityProviderAwareAuthService {
  case class IdpEntry(id: String, issuer: String, service: AuthServiceJWT)
}
