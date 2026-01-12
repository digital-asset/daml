// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdRequest,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import io.grpc.Status
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

final class GetEventsByContractIdRequestAuthIT extends ReadOnlyServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "EventQueryService#GetEventsByContractId"

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectFailure(f, Status.Code.NOT_FOUND)

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    val eventFormatO = context.updateFormat
      .flatMap(_.includeTransactions)
      .flatMap(_.eventFormat)
    stub(EventQueryServiceGrpc.stub(channel), context.token)
      .getEventsByContractId(
        GetEventsByContractIdRequest(
          contractId = LfContractId.V1(ExampleTransactionFactory.lfHash(375)).coid,
          eventFormat = eventFormatO match {
            case None => Some(eventFormat(getMainActorId))
            case Some(_) => eventFormatO
          },
        )
      )
  }

  serviceCallName should {
    "allow calls with eventFormat of the correct party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT with an unknown party authorized to read/write"
        )
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCall(canReadAsMainActor.copy(updateFormat = updateFormat(Some(getMainActorId))))
      )
    }

    "deny calls with eventFormat of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a JWT that cannot read for a party of eventFormat")
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(canActAsRandomParty.copy(updateFormat = updateFormat(Some(getMainActorId))))
      )
    }

    "allow calls with eventFormat of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read as any party of eventFormat"
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            Some(
              UpdateFormat(
                includeTransactions = Some(
                  TransactionFormat(
                    eventFormat = Some(
                      EventFormat(
                        filtersByParty = Map.empty,
                        filtersForAnyParty = Some(Filters(Nil)),
                        verbose = false,
                      )
                    ),
                    transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
                  )
                ),
                includeReassignments = None,
                includeTopologyEvents = None,
              )
            )
          )
        )
      )
    }

    "deny calls with eventFormat of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of eventFormat"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            Some(
              UpdateFormat(
                includeTransactions = Some(
                  TransactionFormat(
                    eventFormat = Some(
                      EventFormat(
                        filtersByParty = Map.empty,
                        filtersForAnyParty = Some(Filters(Nil)),
                        verbose = false,
                      )
                    ),
                    transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
                  )
                ),
                includeReassignments = None,
                includeTopologyEvents = None,
              )
            )
          )
        )
      )
    }
  }

  s"$serviceCallName legacy" should {
    "allow calls with requestingParties of the correct party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT with an unknown party authorized to read/write"
        )
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCall(canReadAsMainActor)
      )
    }

    "deny calls with requestingParties of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of requestingParties"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(canActAsRandomParty)
      )
    }
  }
}
