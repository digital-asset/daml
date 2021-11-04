// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.test.ValueGenerators.{party => partyGen}
import com.daml.extractor.config.CustomScoptReaders._
import com.daml.extractor.config.ExtractorConfig
import com.daml.extractor.services.ExtractorFixtureAroundAll
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalacheck.Arbitrary
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._

class MultiPartySpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript = Some("RecordsAndVariants:multiParty")

  override protected val parties = NonEmptyList(1, 2).map(n => s"MultiParty$n")

  private[this] implicit def partyArb: Arbitrary[Party] = Arbitrary(partyGen)
  private[this] val readParties = implicitly[scopt.Read[ExtractorConfig.Parties]]

  "Party parser" should "permit comma separation" in forAll { parties: OneAnd[List, Party] =>
    readParties.reads(parties.widen[String] intercalate ",") should ===(parties)
  }

  "Party parser" should "permit spaces in parties" in {
    readParties.reads("foo bar,baz quux, a b ") should ===(
      OneAnd("foo bar", List("baz quux", " a b "))
    )
  }

  "Party parser" should "reject non-comma bad characters" in {
    an[IllegalArgumentException] should be thrownBy {
      readParties reads "amazing!"
    }
  }

  "Contracts" should "contain the visible contracts" in {
    val ticks = getContracts.map { ct =>
      for {
        o <- ct.create_arguments.asObject
        tick <- o("tick")
        n <- tick.asNumber
        i <- n.toInt
      } yield i
    }

    val expected = List(1, 2, 4, 5, 7).map(some)

    ticks should contain theSameElementsAs expected
  }
}
