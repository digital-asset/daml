// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.http
import com.digitalasset.canton.http.EndpointsCompanion.Unauthorized
import com.digitalasset.canton.http.{JwtPayload, JwtWritePayload}
import com.digitalasset.daml.lf.value.test.ValueGenerators.party as partyGen
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.scalacheck.ScalazArbitrary.*
import scalaz.std.set.*
import scalaz.syntax.foldable1.*
import scalaz.{-\/, NonEmptyList, \/-}

import Arbitrary.arbitrary

class JwtPartiesTest
    extends AnyWordSpec
    with ScalaFutures
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with OptionValues {
  import JwtPartiesTest.*

  "ensureReadAsAllowedByJwt" should {
    import JwtParties.{ensureReadAsAllowedByJwt, EnsureReadAsDisallowedError}

    "always allow missing readAs" in forAll { (jp: JwtPayload) =>
      ensureReadAsAllowedByJwt(None, jp) should ===(\/-(()))
    }

    "allow any subset" in forAll { (jp: JwtPayload) =>
      val half = NonEmpty.from(jp.parties take (1 max (jp.parties.size / 2))).value
      ensureReadAsAllowedByJwt(Some(half.toNEF.toNel), jp) should ===(\/-(()))
    }

    "disallow any party not in jwt" in forAll { (p: http.Party, jp: JwtPayload) =>
      whenever(!jp.parties(p)) {
        ensureReadAsAllowedByJwt(Some(NonEmptyList(p)), jp) should ===(
          -\/(Unauthorized(s"$EnsureReadAsDisallowedError: $p"))
        )
      }
    }
  }

  "resolveRefParties" should {
    import JwtParties.resolveRefParties

    // ensures compatibility with old behavior
    "use Jwt if explicit spec is absent" in forAll { (jwp: JwtWritePayload) =>
      discard(resolveRefParties(None, jwp) should ===(jwp.parties))
      resolveRefParties(
        Some(http.CommandMeta(None, None, None, None, None, None, None, None, None)),
        jwp,
      ) should ===(
        jwp.parties
      )
    }

    "ignore Jwt if full explicit spec is present" in forAll {
      (actAs: NonEmptyList[http.Party], readAs: List[http.Party], jwp: JwtWritePayload) =>
        resolveRefParties(
          Some(partiesOnlyMeta(actAs = actAs, readAs = readAs)),
          jwp,
        ) should ===(actAs.toSet1 ++ readAs)
    }
  }
}

object JwtPartiesTest {
  private val irrelevantAppId = http.ApplicationId("bar")

  private implicit val arbParty: Arbitrary[http.Party] = Arbitrary(
    http.Party.subst(partyGen: Gen[String])
  )

  private implicit val arbJwtR: Arbitrary[JwtPayload] =
    Arbitrary(arbitrary[(Boolean, http.Party, List[http.Party], List[http.Party])].map {
      case (neAct, extra, actAs, readAs) =>
        http
          .JwtPayload(
            irrelevantAppId,
            actAs = if (neAct) extra :: actAs else actAs,
            readAs = if (!neAct) extra :: readAs else readAs,
          )
          .getOrElse(sys.error("should have satisfied JwtPayload invariant"))
    })

  private implicit val arbJwtW: Arbitrary[JwtWritePayload] =
    Arbitrary(
      arbitrary[(NonEmptyList[http.Party], List[http.Party])].map { case (submitter, readAs) =>
        JwtWritePayload(
          irrelevantAppId,
          submitter = submitter,
          readAs = readAs,
        )
      }
    )

  private[http] def partiesOnlyMeta(actAs: NonEmptyList[http.Party], readAs: List[http.Party]) =
    http.CommandMeta(
      commandId = None,
      actAs = Some(actAs),
      readAs = Some(readAs),
      submissionId = None,
      workflowId = None,
      deduplicationPeriod = None,
      disclosedContracts = None,
      synchronizerId = None,
      packageIdSelectionPreference = None,
    )
}
