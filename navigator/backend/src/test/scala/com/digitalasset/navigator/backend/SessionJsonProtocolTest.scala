// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import com.daml.navigator.model.PartyState
import org.scalatest.{FlatSpec, Matchers}
import SessionJsonProtocol.userWriter
import com.daml.ledger.api.refinements.ApiTypes
import spray.json.{JsBoolean, JsObject, JsString}

class SessionJsonProtocolTest extends FlatSpec with Matchers {

  val userClassName = User.getClass.getSimpleName
  val party = ApiTypes.Party("party")

  behavior of s"JsonCodec[$userClassName]"

  it should s"encode $userClassName without role" in {
    val user = User(id = "id", party = new PartyState(party, false), canAdvanceTime = true)
    val userJson = JsObject(
      "id" -> JsString("id"),
      "party" -> JsString("party"),
      "canAdvanceTime" -> JsBoolean(true))
    userWriter.write(user) shouldEqual userJson
  }

  it should s"encode $userClassName with role" in {
    val user = User(
      id = "id",
      party = new PartyState(party, false),
      role = Some("role"),
      canAdvanceTime = false)
    val userJson = JsObject(
      "id" -> JsString("id"),
      "role" -> JsString("role"),
      "party" -> JsString("party"),
      "canAdvanceTime" -> JsBoolean(false))
    userWriter.write(user) shouldEqual userJson
  }
}
