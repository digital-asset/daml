// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.{
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommand
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class GetUpdatesAuthIT
    extends ExpiringStreamServiceCallAuthTests[GetUpdatesResponse]
    with ReadOnlyServiceCallAuthTests
    with SubmitAndWaitDummyCommand {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UpdateService#GetUpdates"

  private def request(updateFormat: Option[UpdateFormat]) =
    new GetUpdatesRequest(
      beginExclusive = participantBegin,
      endInclusive = None,
      updateFormat = updateFormat,
    )

  override protected def stream(
      context: ServiceCallContext,
      env: TestConsoleEnvironment,
  ): StreamObserver[GetUpdatesResponse] => Unit =
    observer =>
      stub(UpdateServiceGrpc.stub(channel), context.token)
        .getUpdates(request(context.updateFormat), observer)

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    val mainActorId = getMainActorId
    submitAndWaitAsMainActor(mainActorId).flatMap { _ =>
      val contextWithUpdateFormat =
        if (context.updateFormat.isDefined) context
        else context.copy(updateFormat = updateFormat(Some(mainActorId)))
      new StreamConsumer[GetUpdatesResponse](
        stream(contextWithUpdateFormat, env)
      ).first()
    }
  }

  serviceCallName should {
    "allow calls with transaction filters of the correct party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT with an unknown party authorized to read/write"
        )
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(canReadAsMainActor.copy(updateFormat = updateFormat(Some(getMainActorId))))
      )
    }

    "deny calls with transaction filters of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of transaction filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(canActAsRandomParty.copy(updateFormat = updateFormat(Some(getMainActorId))))
      )
    }

    "deny calls with reassignment filters of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of reassignment filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = Some(getRandomPartyId),
            )
          )
        )
      )
    }

    "deny calls with topology filters of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for the single party of topology filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq(getRandomPartyId)),
            )
          )
        )
      )
    }

    "deny calls with topology filters of the wrong party along with the correct" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of topology filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq(getMainActorId, getRandomPartyId)),
            )
          )
        )
      )
    }

    "deny calls with topology filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of topology filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq.empty),
            )
          )
        )
      )
    }

    "allow calls with transaction filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read as any party of transaction filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
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

    "deny calls with transaction filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of transaction filters"
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

    "deny calls with reassignment filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of reassignment filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            Some(
              UpdateFormat(
                includeTransactions = None,
                includeReassignments = Some(
                  EventFormat(
                    filtersByParty = Map.empty,
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = false,
                  )
                ),
                includeTopologyEvents = None,
              )
            )
          )
        )
      )
    }

    "allow calls with reassignment filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read as any party of reassignment filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            updateFormat(Some(getMainActorId)).map(
              _.update(
                _.includeReassignments.filtersForAnyParty := Filters(Nil)
              )
            )
          )
        )
      )
    }

    "allow calls with topology filters of party wildcard with a can-read-as-any-party user" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that can read as any party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq.empty),
            )
          )
        )
      )
    }

    "allow calls with topology filters of several parties with a can-read-as-any-party user" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that can read as any party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq(getMainActorId, getRandomPartyId)),
            )
          )
        )
      )
    }
  }
}
