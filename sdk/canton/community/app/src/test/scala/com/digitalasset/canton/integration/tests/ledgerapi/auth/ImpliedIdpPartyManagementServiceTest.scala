// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

class ImpliedIdpPartyManagementServiceTest extends ServiceCallAuthTests with ImpliedIdpFixture {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "<N/A>"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  "user management service" should {

    "imply idp id from access token if idp not specified in the request and user is an idp admin" in {
      implicit env =>
        val idpId1 = "idp1-" + UUID.randomUUID().toString
        impliedIdpWithIdpAdminFixture(idpId1) { (context: ServiceCallContext) =>
          import env.*

          def assertExpectedIdpF(
              expectedParty: String
          )(msg: String, details: PartyDetails): Assertion = {
            details.party shouldBe expectedParty withClue msg
            details.identityProviderId shouldBe idpId1 withClue msg
          }

          val partyManagement =
            stub(PartyManagementServiceGrpc.stub(channel), context.token)

          loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
            for {
              allocatePartyResp <- partyManagement.allocateParty(
                AllocatePartyRequest(
                  partyIdHint = "",
                  localMetadata = None,
                  identityProviderId = "",
                  synchronizerId = "",
                  userId = "",
                )
              )
              party = allocatePartyResp.partyDetails.value.party
              assertExpectedIdp = assertExpectedIdpF(expectedParty = party) _
              _ = assertExpectedIdp("allocate party", allocatePartyResp.partyDetails.value)
              getPartiesResp <- partyManagement.getParties(
                GetPartiesRequest(
                  parties = Seq(party),
                  identityProviderId = "",
                )
              )
              _ = assertExpectedIdp("get parties", getPartiesResp.partyDetails.headOption.value)
              listKnownPartiesResp <- partyManagement.listKnownParties(
                ListKnownPartiesRequest(
                  pageToken = "",
                  pageSize = 0,
                  identityProviderId = "",
                )
              )
              _ = assertExpectedIdp(
                "list known parties",
                listKnownPartiesResp.partyDetails.find(_.party == party).value,
              )
              _ <- partyManagement.updatePartyDetails(
                UpdatePartyDetailsRequest(
                  Some(
                    PartyDetails(
                      party = party,
                      isLocal = false,
                      localMetadata =
                        Some(ObjectMeta(resourceVersion = "", annotations = Map("foo" -> "bar"))),
                      identityProviderId = "",
                    )
                  ),
                  updateMask = Some(FieldMask(paths = Seq("local_metadata.annotations"))),
                )
              )
            } yield List(party) -> List.empty
          }
        }
    }

    "not imply idp id from access token if user is an admin" in { implicit env =>
      val idpId1 = "idp1-" + UUID.randomUUID().toString
      impliedIdpWithParticipantAdminFixture(idpId1) { (context: ServiceCallContext) =>
        import env.*

        def assertExpectedIdpF(
            expectedParty: String
        )(msg: String, details: PartyDetails): Assertion = {
          details.party shouldBe expectedParty withClue msg
          details.identityProviderId shouldBe "" withClue msg
        }

        val partyManagement =
          stub(PartyManagementServiceGrpc.stub(channel), context.token)

        for {
          allocatePartyResp <- partyManagement.allocateParty(
            AllocatePartyRequest(
              partyIdHint = "",
              localMetadata = None,
              identityProviderId = "",
              synchronizerId = "",
              userId = "",
            )
          )
          party = allocatePartyResp.partyDetails.value.party
          assertExpectedIdp = assertExpectedIdpF(expectedParty = party) _
          _ = assertExpectedIdp("allocate party", allocatePartyResp.partyDetails.value)
          getPartiesResp <- partyManagement.getParties(
            GetPartiesRequest(
              parties = Seq(party),
              identityProviderId = "",
            )
          )
          _ = assertExpectedIdp("get parties", getPartiesResp.partyDetails.headOption.value)
          listKnownPartiesResp <- partyManagement.listKnownParties(
            ListKnownPartiesRequest(
              pageToken = "",
              pageSize = 0,
              identityProviderId = "",
            )
          )
          _ = assertExpectedIdp(
            "list known parties",
            listKnownPartiesResp.partyDetails.find(_.party == party).value,
          )
          _ <- partyManagement.updatePartyDetails(
            UpdatePartyDetailsRequest(
              Some(
                PartyDetails(
                  party = party,
                  isLocal = false,
                  localMetadata =
                    Some(ObjectMeta(resourceVersion = "", annotations = Map("foo" -> "bar"))),
                  identityProviderId = "",
                )
              ),
              updateMask = Some(FieldMask(paths = Seq("local_metadata.annotations"))),
            )
          )
        } yield succeed
      }
    }

  }
}
