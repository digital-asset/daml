package com.daml.http
package util

import domain.{JwtPayload, JwtWritePayload}
import com.daml.lf.value.test.ValueGenerators.{party => partyGen}

import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.{\/-, NonEmptyList}
import scalaz.scalacheck.ScalazArbitrary._

class JwtPartiesTest
    extends AnyWordSpec
    with ScalaFutures
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  import JwtPartiesTest._

  "ensureReadAsAllowedByJwt" should {
    import JwtParties.ensureReadAsAllowedByJwt

    "always allow missing readAs" in forAll { jp: JwtPayload =>
      ensureReadAsAllowedByJwt(None, jp) should ===(\/-(()))
    }
  }

  "resolveRefParties" should {
    import JwtParties.resolveRefParties

    // ensures compatibility with old behavior
    "use Jwt if explicit spec is absent" in forAll { jwp: JwtWritePayload =>
      resolveRefParties(None, jwp) should ===(jwp.parties)
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
}
