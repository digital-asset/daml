// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.digitalasset.canton.http.domain.{JwtPayload, JwtWritePayload}
import com.digitalasset.canton.http.EndpointsCompanion.Unauthorized
import com.daml.lf.value.test.ValueGenerators.party as partyGen
import com.daml.scalautil.Statement.discard
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import com.digitalasset.canton.http.domain
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.{-\/, NonEmptyList, \/-}
import scalaz.std.set.*
import scalaz.syntax.foldable1.*
import scalaz.scalacheck.ScalazArbitrary.*

class JwtPartiesTest
    extends AnyWordSpec
    with ScalaFutures
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  import JwtPartiesTest._

  "ensureReadAsAllowedByJwt" should {
    import JwtParties.{ensureReadAsAllowedByJwt, EnsureReadAsDisallowedError}

    "always allow missing readAs" in forAll { jp: JwtPayload =>
      ensureReadAsAllowedByJwt(None, jp) should ===(\/-(()))
    }

    "allow any subset" in forAll { jp: JwtPayload =>
      val NonEmpty(half) = jp.parties take (1 max (jp.parties.size / 2))
      ensureReadAsAllowedByJwt(Some(half.toNEF.toNel), jp) should ===(\/-(()))
    }

    "disallow any party not in jwt" in forAll { (p: domain.Party, jp: JwtPayload) =>
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
    "use Jwt if explicit spec is absent" in forAll { jwp: JwtWritePayload =>
      discard(resolveRefParties(None, jwp) should ===(jwp.parties))
      resolveRefParties(
        Some(domain.CommandMeta(None, None, None, None, None, None, None)),
        jwp,
      ) should ===(
        jwp.parties
      )
    }

    "ignore Jwt if full explicit spec is present" in forAll {
      (actAs: NonEmptyList[domain.Party], readAs: List[domain.Party], jwp: JwtWritePayload) =>
        resolveRefParties(
          Some(partiesOnlyMeta(actAs = actAs, readAs = readAs)),
          jwp,
        ) should ===(actAs.toSet1 ++ readAs)
    }
  }
}

object JwtPartiesTest {
  private val irrelevantLedgerId = domain.LedgerId("foo")
  private val irrelevantAppId = domain.ApplicationId("bar")

  private implicit val arbParty: Arbitrary[domain.Party] = Arbitrary(
    domain.Party.subst(partyGen: Gen[String])
  )

  private implicit val arbJwtR: Arbitrary[JwtPayload] =
    Arbitrary(arbitrary[(Boolean, domain.Party, List[domain.Party], List[domain.Party])].map {
      case (neAct, extra, actAs, readAs) =>
        domain
          .JwtPayload(
            irrelevantLedgerId,
            irrelevantAppId,
            actAs = if (neAct) extra :: actAs else actAs,
            readAs = if (!neAct) extra :: readAs else readAs,
          )
          .getOrElse(sys.error("should have satisfied JwtPayload invariant"))
    })

  private implicit val arbJwtW: Arbitrary[JwtWritePayload] =
    Arbitrary(
      arbitrary[(NonEmptyList[domain.Party], List[domain.Party])].map { case (submitter, readAs) =>
        JwtWritePayload(
          irrelevantLedgerId,
          irrelevantAppId,
          submitter = submitter,
          readAs = readAs,
        )
      }
    )

  private[http] def partiesOnlyMeta(actAs: NonEmptyList[domain.Party], readAs: List[domain.Party]) =
    domain.CommandMeta(
      commandId = None,
      actAs = Some(actAs),
      readAs = Some(readAs),
      submissionId = None,
      workflowId = None,
      deduplicationPeriod = None,
      disclosedContracts = None,
    )
}
