// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import org.scalatest.{FlatSpec, Matchers, OptionValues}
import scopt.OptionParser

final class ConfigSpec extends FlatSpec with Matchers with OptionValues {

  private val DumpIndexMetadataCommand = "dump-index-metadata"
  private val ConfigParser: Seq[String] => Option[Config[Unit]] =
    Config.parse("Test", (_: OptionParser[Config[Unit]]) => (), (), _)

  behavior of "Runner"

  it should "fail if a participant is not provided in run mode" in {
    ConfigParser(Seq.empty) shouldEqual None
  }

  it should "fail if a participant is not provided when dumping the index metadata" in {
    ConfigParser(Seq(DumpIndexMetadataCommand)) shouldEqual None
  }

  it should "succeed if a participant is provided when dumping the index metadata" in {
    ConfigParser(Seq(DumpIndexMetadataCommand, "some-jdbc-url"))
  }

  it should "succeed if more than one participant is provided when dumping the index metadata" in {
    ConfigParser(Seq(DumpIndexMetadataCommand, "some-jdbc-url", "some-other-jdbc-url"))
  }

}
