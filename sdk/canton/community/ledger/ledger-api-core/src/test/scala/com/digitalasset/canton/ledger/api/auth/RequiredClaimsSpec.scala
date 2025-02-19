// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TopologyFormat,
  TransactionFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.auth.RequiredClaim
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.lenses.Lens

class RequiredClaimsSpec extends AsyncFlatSpec with BaseTest with Matchers {

  behavior of "submissionClaims"

  it should "compute the correct claims in the happy path" in {
    val applicationIdL: Lens[String, String] = Lens.unit
    RequiredClaims.submissionClaims(
      actAs = Set("1", "2", "3"),
      readAs = Set("a", "b", "c"),
      applicationIdL = applicationIdL,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ActAs("3"),
      RequiredClaim.MatchApplicationId(applicationIdL),
    )
  }

  it should "compute the correct claims if no actAs" in {
    val applicationIdL: Lens[String, String] = Lens.unit
    RequiredClaims.submissionClaims(
      actAs = Set.empty,
      readAs = Set("a", "b", "c"),
      applicationIdL = applicationIdL,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.MatchApplicationId(applicationIdL),
    )
  }

  it should "compute the correct claims if no ActAs and no ReadAs" in {
    val applicationIdL: Lens[String, String] = Lens.unit
    RequiredClaims.submissionClaims(
      actAs = Set.empty,
      readAs = Set.empty,
      applicationIdL = applicationIdL,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.MatchApplicationId(applicationIdL)
    )
  }

  behavior of "transactionFilterClaims"

  it should "compute the correct claims in the happy path" in {
    RequiredClaims.transactionFilterClaims[String](
      TransactionFilter(
        filtersByParty = Map(
          "a" -> Filters(),
          "b" -> Filters(),
          "c" -> Filters(),
        ),
        filtersForAnyParty = Some(Filters()),
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no any party filters" in {
    RequiredClaims.transactionFilterClaims[String](
      TransactionFilter(
        filtersByParty = Map(
          "a" -> Filters(),
          "b" -> Filters(),
          "c" -> Filters(),
        ),
        filtersForAnyParty = None,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
    )
  }

  it should "compute the correct claims if no any party filters and no party filters" in {
    RequiredClaims.transactionFilterClaims[String](
      TransactionFilter(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
      )
    ) shouldBe Nil
  }

  behavior of "readAsForAllParties"

  it should "compute the correct claims in the happy path" in {
    RequiredClaims.readAsForAllParties[String](
      Seq("a", "b", "c")
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
    )
  }

  it should "compute the correct claims if input is empty" in {
    RequiredClaims.readAsForAllParties[String](
      Nil
    ) shouldBe Nil
  }

  behavior of "eventFormatClaims"

  it should "compute the correct claims in the happy path" in {
    RequiredClaims.eventFormatClaims[String](
      EventFormat(
        filtersByParty = Map(
          "a" -> Filters(),
          "b" -> Filters(),
          "c" -> Filters(),
        ),
        filtersForAnyParty = Some(Filters()),
        verbose = true,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no any party filters" in {
    RequiredClaims.eventFormatClaims[String](
      EventFormat(
        filtersByParty = Map(
          "a" -> Filters(),
          "b" -> Filters(),
          "c" -> Filters(),
        ),
        filtersForAnyParty = None,
        verbose = false,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
    )
  }

  it should "compute the correct claims if no any party filters and no party filters" in {
    RequiredClaims.eventFormatClaims[String](
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
        verbose = true,
      )
    ) shouldBe Nil
  }

  behavior of "updateFormatClaims"

  it should "compute the correct claims in the happy path" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = Some(Filters()),
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = Some(
          EventFormat(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
        includeTopologyEvents = Some(
          TopologyFormat(
            includeParticipantAuthorizationEvents = Some(
              ParticipantAuthorizationTopologyFormat(
                parties = Seq("A", "B", "C")
              )
            )
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("A"),
      RequiredClaim.ReadAs("B"),
      RequiredClaim.ReadAs("C"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if empty parties for ParticipantAuthorizationTopologyFormat" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = Some(Filters()),
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = Some(
          EventFormat(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
        includeTopologyEvents = Some(
          TopologyFormat(
            includeParticipantAuthorizationEvents = Some(
              ParticipantAuthorizationTopologyFormat(
                parties = Nil
              )
            )
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if empty ParticipantAuthorizationTopologyFormat" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = Some(Filters()),
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = Some(
          EventFormat(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
        includeTopologyEvents = Some(
          TopologyFormat(
            includeParticipantAuthorizationEvents = None
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if empty TopologyFormat" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = Some(Filters()),
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = Some(
          EventFormat(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
        includeTopologyEvents = None,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no filtersForAnyParty in includeReassignments" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = Some(Filters()),
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = Some(
          EventFormat(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = None,
            verbose = true,
          )
        ),
        includeTopologyEvents = None,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no filtersForAnyParty in includeReassignments and in includeTransactions" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = None,
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = Some(
          EventFormat(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = None,
            verbose = true,
          )
        ),
        includeTopologyEvents = None,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
    )
  }

  it should "compute the correct claims if no includeReassignments" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  "a" -> Filters(),
                  "b" -> Filters(),
                  "c" -> Filters(),
                ),
                filtersForAnyParty = None,
                verbose = true,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = None,
        includeTopologyEvents = None,
      )
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
    )
  }

  it should "compute the correct claims if no includeTransactions" in {
    RequiredClaims.updateFormatClaims[String](
      UpdateFormat(
        includeTransactions = None,
        includeReassignments = None,
        includeTopologyEvents = None,
      )
    ) shouldBe Nil
  }

  behavior of "idpAdminClaimsAndMatchingRequestIdpId"

  it should "compute the correct claims in the happy path" in {
    val identityProviderIdL: Lens[String, String] = Lens.unit
    RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId[String](
      identityProviderIdL = identityProviderIdL,
      mustBeParticipantAdmin = false,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
      RequiredClaim.AdminOrIdpAdmin(),
    )
  }

  it should "compute the correct claims if must be participant admin" in {
    val identityProviderIdL: Lens[String, String] = Lens.unit
    RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId[String](
      identityProviderIdL = identityProviderIdL,
      mustBeParticipantAdmin = true,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
      RequiredClaim.Admin(),
    )
  }

}
