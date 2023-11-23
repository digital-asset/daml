// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.rxjava.grpc.helpers.TestConfiguration
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.ledger.javaapi.data._
import com.daml.ledger.rxjava._
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

class EventQueryClientImplTest
    extends AnyFlatSpec
    with Matchers
    with AuthMatchers
    with OptionValues
    with DataLayerHelpers {

  val ledgerServices = new LedgerServices("event-query-service-ledger")

  private def withEventQueryClient(authService: AuthService = AuthServiceWildcard) = {
    ledgerServices.withEventQueryClient(
      Future.successful(GetEventsByContractIdResponse.defaultInstance),
      Future.successful(GetEventsByContractKeyResponse.defaultInstance),
      authService,
    ) _
  }

  private val contractKey = Bool.TRUE
  private val identifier = new Identifier("recordPackageId", "recordModuleName", "recordEntityName")
  private val contractId = "contract_id"
  private val parties = java.util.Set.of("party_1")

  behavior of "EventQueryClientImpl"

  it should "send the right requests/order" in {
    withEventQueryClient() { (client, service) =>
      client
        .getEventsByContractId(contractId, parties)
        .blockingGet()
      service.getLastGetEventsByContractIdRequest.value.contractId shouldBe contractId
      service.getLastGetEventsByContractIdRequest.value.requestingParties.toSet shouldBe parties.asScala

      client
        .getEventsByContractKey(contractKey, identifier, parties, "1")
        .blockingGet()
      service.getLastGetEventsByContractKeyRequest.value.contractKey
        .flatMap(_.sum.bool) shouldBe contractKey.asBool().toScala.map(_.getValue)
      service.getLastGetEventsByContractKeyRequest.value.templateId.map(_.packageId) shouldBe Some(
        identifier.getPackageId
      )
      service.getLastGetEventsByContractKeyRequest.value.templateId.map(_.entityName) shouldBe Some(
        identifier.getEntityName
      )
      service.getLastGetEventsByContractKeyRequest.value.templateId.map(_.moduleName) shouldBe Some(
        identifier.getModuleName
      )
      service.getLastGetEventsByContractKeyRequest.value.requestingParties.toSet shouldBe parties.asScala
      service.getLastGetEventsByContractKeyRequest.value.continuationToken shouldBe "1"
      client
        .getEventsByContractKey(contractKey, identifier, parties, "")
        .blockingGet()
      service.getLastGetEventsByContractKeyRequest.value.continuationToken shouldBe ""

    }
  }

  behavior of "Security"

  it should "deny access without token" in {
    withEventQueryClient(mockedAuthService) { (client, _) =>
      withClue("getEventsByContractId") {
        expectUnauthenticated {
          client
            .getEventsByContractId(contractId, parties)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        }
      }
      withClue("getEventsByContractKey") {
        expectUnauthenticated {
          client
            .getEventsByContractKey(contractKey, identifier, parties, "")
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        }
      }
    }
  }

  it should "deny access with the wrong token" in {
    withEventQueryClient(mockedAuthService) { (client, _) =>
      withClue("getEventsByContractId") {
        expectPermissionDenied {
          client
            .getEventsByContractId(contractId, parties, someOtherPartyReadWriteToken)
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        }
      }
      withClue("getEventsByContractKey") {
        expectPermissionDenied {
          client
            .getEventsByContractKey(
              contractKey,
              identifier,
              parties,
              "",
              someOtherPartyReadWriteToken,
            )
            .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
            .blockingGet()
        }
      }
    }
  }

  it should "allow access with the right token" in {
    withEventQueryClient() { (client, _) =>
      withClue("getEventsByContractId") {
        client
          .getEventsByContractId(contractId, parties, somePartyReadWriteToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
      }
      withClue("getEventsByContractKey") {
        client
          .getEventsByContractKey(contractKey, identifier, parties, "", somePartyReadWriteToken)
          .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
          .blockingGet()
      }
    }
  }

}
