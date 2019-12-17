// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.http.Generators.{contractGen, contractLocatorGen}
import com.digitalasset.http.domain
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Inside, Matchers}
import scalaz.{\/, \/-}
import spray.json.{JsObject, JsNumber, JsValue}

class JsonProtocolTest
    extends FreeSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks {

  import JsonProtocol._

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  "domain.Contract" - {

    "can be serialized to JSON" in forAll(contractGen) { contract =>
      inside(SprayJson.encode(contract)) {
        case \/-(JsObject(fields)) =>
          inside(fields.toList) {
            case List(("archived", JsObject(_))) =>
            case List(("created", JsObject(_))) =>
          }
      }
    }

    "can be serialized and deserialized back to the same object" in forAll(contractGen) {
      contract0 =>
        val actual: SprayJson.Error \/ domain.Contract[JsValue] = for {
          jsValue <- SprayJson.encode(contract0)
          contract <- SprayJson.decode[domain.Contract[JsValue]](jsValue)
        } yield contract

        inside(actual) {
          case \/-(contract1) => contract1 shouldBe contract0
        }
    }
  }

  "domain.ContractLocator" - {
    type Loc = domain.ContractLocator[JsValue]
    "roundtrips" in forAll(contractLocatorGen(arbitrary[Int] map (JsNumber(_)))) { locator: Loc =>
      import spray.json._
      locator.toJson.convertTo[Loc] should ===(locator)
    }
  }
}
