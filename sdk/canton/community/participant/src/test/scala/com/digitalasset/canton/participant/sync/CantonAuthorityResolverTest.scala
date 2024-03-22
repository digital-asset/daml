// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver.AuthorityRequest
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.{
  AuthorityOfDelegation,
  AuthorityOfResponse,
}
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.Future

class CantonAuthorityResolverTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {
  class Fixture() {
    val p1 = LfPartyId.assertFromString("p1")
    val p2 = LfPartyId.assertFromString("p2")
    val p3 = LfPartyId.assertFromString("p3")
    val p4 = LfPartyId.assertFromString("p4")
    val domainsLookup = mock[ConnectedDomainsLookup]
    val authorityResolver = new CantonAuthorityResolver(domainsLookup, loggerFactory)
    val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domainId::domainId"))
    val syncDomain = mock[SyncDomain]
  }

  private def requestAuthorisation(
      request: AuthorityRequest,
      response: Map[LfPartyId, AuthorityOfDelegation],
  )(implicit f: Fixture): Future[AuthorityResolver.AuthorityResponse] = {
    import f.*
    when(syncDomain.authorityOfInSnapshotApproximation(any[Set[LfPartyId]])(any[TraceContext]))
      .thenReturn(
        Future.successful(
          AuthorityOfResponse(
            response
          )
        )
      )

    authorityResolver
      .resolve(request)
  }

  override def withFixture(test: OneArgTest): Outcome =
    test(new Fixture())

  override type FixtureParam = Fixture

  "CantonAuthorityResolver" should {
    "provide authority in the simple cases" in { implicit f =>
      import f.*
      when(domainsLookup.get(any[DomainId])).thenReturn(Some(syncDomain))
      when(domainsLookup.snapshot).thenReturn(Map(domainId -> syncDomain))

      requestAuthorisation(
        AuthorityRequest(holding = Set(p2), requesting = Set(p1), domainId = None),
        Map(p1 -> AuthorityOfDelegation(Set(p2), PositiveInt.tryCreate(1))),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.Authorized

      requestAuthorisation(
        AuthorityRequest(holding = Set(p2, p3), requesting = Set(p1), domainId = None),
        Map(p1 -> AuthorityOfDelegation(Set(p2, p3), PositiveInt.tryCreate(2))),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.Authorized

      requestAuthorisation(
        AuthorityRequest(holding = Set(p2, p3), requesting = Set(p1), domainId = None),
        Map(p1 -> AuthorityOfDelegation(Set(p1, p2, p3), PositiveInt.tryCreate(2))),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.Authorized

      requestAuthorisation(
        AuthorityRequest(holding = Set(p2, p3, p4), requesting = Set(p1), domainId = None),
        Map(p1 -> AuthorityOfDelegation(Set(p2, p3), PositiveInt.tryCreate(2))),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.Authorized

      requestAuthorisation(
        AuthorityRequest(holding = Set(p3, p4), requesting = Set(p1, p2), domainId = None),
        Map(
          p1 -> AuthorityOfDelegation(Set(p3, p4), PositiveInt.tryCreate(2)),
          p2 -> AuthorityOfDelegation(Set(p4), PositiveInt.tryCreate(1)),
        ),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.Authorized
    }

    "do not provide authority in the simplest case" in { implicit f =>
      import f.*
      when(domainsLookup.get(any[DomainId])).thenReturn(Some(syncDomain))
      when(domainsLookup.snapshot).thenReturn(Map(domainId -> syncDomain))

      requestAuthorisation(
        AuthorityRequest(holding = Set(p2), requesting = Set(p1), domainId = None),
        Map(p1 -> AuthorityOfDelegation(Set.empty, PositiveInt.tryCreate(1))),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.MissingAuthorisation(Set(p1))

      requestAuthorisation(
        AuthorityRequest(holding = Set.empty, requesting = Set(p1), domainId = None),
        Map(p1 -> AuthorityOfDelegation(Set.empty, PositiveInt.tryCreate(1))),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.MissingAuthorisation(Set(p1))

      requestAuthorisation(
        AuthorityRequest(holding = Set(p2), requesting = Set(p1), domainId = None),
        Map(),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.MissingAuthorisation(Set(p1))

      requestAuthorisation(
        AuthorityRequest(holding = Set(p3, p4), requesting = Set(p1, p2), domainId = None),
        Map(
          p1 -> AuthorityOfDelegation(Set(p1), PositiveInt.tryCreate(2)),
          p2 -> AuthorityOfDelegation(Set(p2), PositiveInt.tryCreate(1)),
        ),
      ).futureValue shouldBe AuthorityResolver.AuthorityResponse.MissingAuthorisation(Set(p1, p2))
    }

    "throw if no connected domains" in { implicit f =>
      import f.*
      when(domainsLookup.snapshot).thenReturn(Map.empty)

      loggerFactory.assertInternalErrorAsync[Exception](
        requestAuthorisation(
          AuthorityRequest(holding = Set(p2), requesting = Set(p1), domainId = None),
          Map(p1 -> AuthorityOfDelegation(Set(p2), PositiveInt.tryCreate(1))),
        ),
        _.getMessage shouldBe "No connected domains",
      )
    }

    "throw if domain is not recognized" in { implicit f =>
      import f.*
      when(domainsLookup.get(any[DomainId])).thenReturn(None)

      loggerFactory.assertInternalErrorAsync[Exception](
        requestAuthorisation(
          AuthorityRequest(
            holding = Set(p2),
            requesting = Set(p1),
            domainId = Some(domainId),
          ),
          Map(p1 -> AuthorityOfDelegation(Set(p2), PositiveInt.tryCreate(1))),
        ),
        _.getMessage shouldBe show"$domainId is unrecognized",
      )
    }

    "throw if requesting parties are empty" in { f =>
      import f.*
      when(syncDomain.authorityOfInSnapshotApproximation(any[Set[LfPartyId]])(any[TraceContext]))
        .thenReturn(Future.successful(AuthorityOfResponse(Map.empty)))

      loggerFactory.assertInternalErrorAsync[Exception](
        authorityResolver
          .resolve(AuthorityRequest(Set.empty, Set.empty, None)),
        _.getMessage shouldBe "Empty authority requesting parties",
      )
    }
  }
}
