// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.HttpServiceTestFixture.UseTls
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.\/-

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceOffsetTickIntTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns
    with BeforeAndAfterAll {

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def useTls: UseTls = UseTls.NoTls

  // make sure websocket heartbeats non-stop, DO NOT CHANGE `0.second`
  override def wsConfig: Option[WebsocketConfig] =
    Some(Config.DefaultWsConfig.copy(heartBeatPer = 0.second))

  import WebsocketTestFixture._

  "Given empty ACS, JSON API should emit only offset ticks" in withHttpService { (uri, _, _) =>
    for {
      msgs <- singleClientQueryStream(jwt, uri, """{"templateIds": ["Iou:Iou"]}""")
        .take(10)
        .runWith(collectResultsAsTextMessage)
    } yield {
      inside(eventsBlockVector(msgs.toVector)) {
        case \/-(offsetTicks) =>
          offsetTicks.forall(isOffsetTick) shouldBe true
          offsetTicks should have length 10
      }
    }
  }

  "Given non-empty ACS, JSON API should emit ACS block and after it only absolute offset ticks" in withHttpService {
    (uri, _, _) =>
      for {
        _ <- initialIouCreate(uri)

        msgs <- singleClientQueryStream(jwt, uri, """{"templateIds": ["Iou:Iou"]}""")
          .take(10)
          .runWith(collectResultsAsTextMessage)
      } yield {
        inside(eventsBlockVector(msgs.toVector)) {
          case \/-(acs +: offsetTicks) =>
            isAcs(acs) shouldBe true
            acs.events should have length 1
            offsetTicks.forall(isAbsoluteOffsetTick) shouldBe true
            offsetTicks should have length 9
        }
      }
  }
}
