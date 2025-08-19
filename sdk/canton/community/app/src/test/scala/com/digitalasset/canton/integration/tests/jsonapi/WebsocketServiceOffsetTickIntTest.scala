// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.{Offset, WebsocketConfig}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import scalaz.\/-

import scala.concurrent.duration.*

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceOffsetTickIntTest
    extends AbstractHttpServiceIntegrationTestFuns
    with AbstractHttpServiceIntegrationTestFunsUserToken {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  // make sure websocket heartbeats non-stop, DO NOT CHANGE `0.second`
  override def wsConfig: Option[WebsocketConfig] =
    Some(WebsocketConfig(heartbeatPeriod = 0.second))

  import WebsocketTestFixture.*

  "JSON API websocket endpoints" should {
    "emit only offset ticks, given empty ACS" in httpTestFixture { fixture =>
      import fixture.uri
      for {
        jwt <- fixture.jwt(uri)
        msgs <-
          suppressPackageIdWarning {
            singleClientQueryStream(jwt, uri, s"""{"templateIds": ["${TpId.Iou.Iou.fqn}"]}""")
              .take(10)
              .runWith(collectResultsAsTextMessage)
          }
      } yield {
        inside(eventsBlockVector(msgs.toVector)) { case \/-(offsetTicks) =>
          offsetTicks.forall(isOffsetTick) shouldBe true
          offsetTicks should have length 10
        }
      }
    }

    "emit ACS block and after it only absolute offset ticks, given non-empty ACS" in withHttpService() {
      fixture =>
        import fixture.uri

        for {
          aliceHeaders <- fixture.getUniquePartyAndAuthHeaders("Alice")
          (party, headers) = aliceHeaders
          _ <- initialIouCreate(uri, party, headers)
          jwt <- jwtForParties(uri)(List(party), List())
          msgs <-
            suppressPackageIdWarning {
              singleClientQueryStream(jwt, uri, s"""{"templateIds": ["${TpId.Iou.Iou.fqn}"]}""")
                .take(10)
                .runWith(collectResultsAsTextMessage)
            }
        } yield {
          inside(eventsBlockVector(msgs.toVector)) { case \/-(acs +: offsetTicks) =>
            isAcs(acs) shouldBe true
            acs.events should have length 1
            offsetTicks.forall(isAbsoluteOffsetTick) shouldBe true
            offsetTicks should have length 9
          }
        }
    }

    "immediately start emitting ticks, given an offset to resume at" in httpTestFixture { fixture =>
      import fixture.{uri, client}
      for {
        ledgerOffset <- client.stateService
          .getLedgerEndOffset()
          .map(Offset(_))
        jwt <- fixture.jwt(uri)
        msgs <-
          suppressPackageIdWarning {
            singleClientQueryStream(
              jwt,
              uri,
              s"""{"templateIds": ["${TpId.Iou.Iou.fqn}"]}""",
              offset = Some(ledgerOffset),
            )
              .take(10)
              .runWith(collectResultsAsTextMessage)
          }
      } yield {
        inside(eventsBlockVector(msgs.toVector)) { case \/-(offsetTicks) =>
          offsetTicks.forall(isAbsoluteOffsetTick) shouldBe true
          offsetTicks should have length 10
        }
      }
    }

    "immediately start emitting ticks, given an offset to resume at inside the query" in httpTestFixture {
      fixture =>
        import fixture.{uri, client}
        for {
          ledgerOffset <- client.stateService
            .getLedgerEndOffset()
            .map(Offset(_))
          jwt <- fixture.jwt(uri)
          msgs <-
            suppressPackageIdWarning {
              singleClientQueryStream(
                jwt,
                uri,
                s"""[{"templateIds": ["${TpId.Iou.Iou.fqn}"], "offset": "$ledgerOffset"}]""",
              )
                .take(10)
                .runWith(collectResultsAsTextMessage)
            }
        } yield {
          inside(eventsBlockVector(msgs.toVector)) { case \/-(offsetTicks) =>
            offsetTicks.forall(isAbsoluteOffsetTick) shouldBe true
            offsetTicks should have length 10
          }
        }
    }
  }
}
