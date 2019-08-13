// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.ValueGenerators.{party => partyGen}
import config.ExtractorConfig
import config.CustomScoptReaders._
import services.ExtractorFixtureAroundAll
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll

import org.scalacheck.Arbitrary
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import java.io.File
import scopt.Read

import scalaz._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.scalacheck.ScalazArbitrary._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class MultiPartySpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers
    with GeneratorDrivenPropertyChecks {

  override protected def darFile = new File(rlocation("extractor/RecordsAndVariants.dar"))

  override def scenario: Option[String] = Some("RecordsAndVariants:multiParty")

  override def configureExtractor(ec: ExtractorConfig): ExtractorConfig = {
    val ec2 = super.configureExtractor(ec)
    ec2.copy(parties = OneAnd(Party assertFromString "Alice", ec2.parties.toList))
  }

  private[this] implicit def partyArb: Arbitrary[Party] = Arbitrary(partyGen)
  private[this] val readParties = implicitly[Read[ExtractorConfig.Parties]]

  "Party parser" should "permit comma separation" in forAll { parties: OneAnd[List, Party] =>
    readParties.reads(parties.widen[String] intercalate ",") should ===(parties)
  }

  "Party parser" should "permit spaces in parties" in {
    readParties.reads("foo bar,baz quux, a b ") should ===(
      OneAnd("foo bar", List("baz quux", " a b ")))
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
