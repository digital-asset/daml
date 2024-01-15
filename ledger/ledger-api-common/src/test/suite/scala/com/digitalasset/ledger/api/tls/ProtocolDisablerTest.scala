// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.security.Security

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable

class ProtocolDisablerTest extends AnyWordSpec with Matchers {

  "properties updater" should {

    // given
    val hello = "SSLv2Hello"
    val noHello = "SSLv3, RC4, MD5withRSA, DH keySize < 1024, EC keySize < 224"
    val helloAtTheEnd = s"$noHello, $hello"
    val helloAtTheBeginning = s"$hello, $noHello"
    val state: mutable.Map[String, String] = mutable.Map.empty
    val updater = PropertiesUpdater(state(_), state(_) = _)

    "add element if it doesn't exist yet" in {
      // when
      state("without") = noHello
      updater.appendToProperty("without", hello)
      // then
      state("without") shouldBe helloAtTheEnd
    }

    "do nothing if the element exists already" in {
      // when
      state("atTheEnd") = helloAtTheEnd
      state("atTheBeginning") = helloAtTheBeginning
      updater.appendToProperty("atTheEnd", hello)
      updater.appendToProperty("atTheBeginning", hello)
      // then
      state("atTheEnd") shouldBe helloAtTheEnd
      state("atTheBeginning") shouldBe helloAtTheBeginning
    }

    "add the element if it already exists as substring of another element" in {
      // when
      val helloAsSubstring =
        "SSLv3, RC4, MD5withRSA, DH keySize < 1024, EC keySize < 224, SSLv2Hello Hello"
      state("asSubstring") = helloAsSubstring
      updater.appendToProperty("asSubstring", hello)
      // then
      state("asSubstring") shouldBe s"$helloAsSubstring, $hello"
    }
  }

  "protocol disabler" should {
    // given
    def disabledProtocols = Security.getProperty(ProtocolDisabler.disabledAlgorithmsProperty)
    "disable hello protocol if it is enabled" in {
      // when
      val startingValue = disabledProtocols
      val expected = ProtocolDisabler.sslV2Protocol.r.findFirstIn(startingValue) match {
        case None =>
          ProtocolDisabler.disableSSLv2Hello()
          s"$startingValue, ${ProtocolDisabler.sslV2Protocol}"
        case Some(_) =>
          startingValue
      }
      val endingValue = disabledProtocols

      // then
      endingValue shouldBe expected
    }
  }

}
