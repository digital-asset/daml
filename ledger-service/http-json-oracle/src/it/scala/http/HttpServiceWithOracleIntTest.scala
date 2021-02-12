// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.scalatest.Inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json.JsValue

// TODO SC remove `abstract` to reenable
abstract class HttpServiceWithOracleIntTest
    extends AbstractHttpServiceIntegrationTest
    with HttpServiceOracleInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None
}

// TODO SC this is a small subset of above test, remove when reenabling
class HttpServiceWithOracleIntTestStub
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with AbstractHttpServiceIntegrationTestFuns
    with HttpServiceOracleInt {
  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  override def useTls = HttpServiceTestFixture.UseTls.NoTls

  "query POST with empty query" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      List.empty,
      jsObject("""{"templateIds": ["Iou:Iou"]}"""),
      uri,
      encoder,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl shouldBe empty
    }
  }
}
