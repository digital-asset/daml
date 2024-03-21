// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.grpc.GrpcException
import com.daml.integrationtest.CantonFixture
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.AuthServiceJWTCodec
import com.daml.ledger.api.auth.CustomDamlJWTPayload
import com.daml.ledger.client.configuration.{
  LedgerClientConfiguration,
  LedgerIdRequirement,
  CommandClientConfiguration,
}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class LedgerClientAuthIT extends AsyncWordSpec with Matchers with Inside with CantonFixture {

  protected val jwtSecret: String = java.util.UUID.randomUUID.toString

  override protected lazy val authSecret = Some(jwtSecret)

  private val ClientConfigurationWithoutToken = LedgerClientConfiguration(
    applicationId = classOf[LedgerClientAuthIT].getSimpleName,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  private val emptyToken = CustomDamlJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = false,
    actAs = Nil,
    readAs = Nil,
  )

  private val ClientConfiguration = ClientConfigurationWithoutToken.copy(
    token = Some(
      JwtSigner.HMAC256
        .sign(
          DecodedJwt(
            """{"alg": "HS256", "typ": "JWT"}""",
            AuthServiceJWTCodec.compactPrint(emptyToken.copy(readAs = List("Alice")), None),
          ),
          jwtSecret,
        )
        .getOrElse(sys.error("Failed to generate token"))
        .value
    )
  )

  lazy val channel = config.channel(ports.head)

  "the ledger client" when {
    "it has a read-only token" should {
      "retrieve the ledger ID" in {
        for {
          client <- LedgerClient(channel, ClientConfiguration)
        } yield {
          client.ledgerId should be(config.ledgerIds.head)
        }
      }

      "fail to conduct an admin operation with the same token" in {
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          exception <- client.partyManagementClient
            .allocateParty(hint = Some("Bob"), displayName = None)
            .failed
        } yield {
          inside(exception) { case GrpcException.PERMISSION_DENIED() =>
            succeed
          }
        }
      }

      "succeed in conducting an admin operation with an admin token" in {
        val partyName = "Carol"
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          allocatedParty <- client.partyManagementClient
            .allocateParty(
              hint = Some(partyName),
              displayName = Some(partyName),
              token = config.adminToken,
            )
        } yield {
          allocatedParty.displayName should be(Some(partyName))
        }
      }
    }

    "it does not have a token" should {
      "fail to construct" in {
        for {
          exception <- LedgerClient(channel, ClientConfigurationWithoutToken).failed
        } yield {
          inside(exception) { case GrpcException.UNAUTHENTICATED() =>
            succeed
          }
        }
      }
    }
  }
}
