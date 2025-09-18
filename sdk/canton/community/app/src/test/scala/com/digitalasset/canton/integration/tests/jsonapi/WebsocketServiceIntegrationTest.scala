// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.json.JsonProtocol
import com.digitalasset.canton.http.{
  ContractId,
  ContractKeyStreamRequest,
  ContractTypeId as HttpContractTypeId,
  EnrichedContractKey,
  WebsocketConfig,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.WebsocketTestFixture.*
import com.digitalasset.daml.lf.data.Ref.PackageRef
import spray.json.{
  DeserializationException,
  JsNull,
  JsObject,
  JsString,
  JsValue,
  enrichAny as `sj enrichAny`,
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class WebsocketServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTestFuns
    with AbstractHttpServiceIntegrationTestFunsUserToken {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def wsConfig: Option[WebsocketConfig] = Some(WebsocketConfig())

  "ContractKeyStreamRequest" when {
    import JsonProtocol.*
    val baseVal =
      EnrichedContractKey(
        HttpContractTypeId.Template(PackageRef.assertFromString("ab"), "cd", "ef"),
        JsString("42"): JsValue,
      )
    val baseMap = baseVal.toJson.asJsObject.fields
    val withSome = JsObject(baseMap + (contractIdAtOffsetKey -> JsString("hi")))
    val withNone = JsObject(baseMap + (contractIdAtOffsetKey -> JsNull))

    "initial JSON reader" should {
      type T = ContractKeyStreamRequest[Unit, JsValue]

      "shares EnrichedContractKey format" in { _ =>
        JsObject(baseMap).convertTo[T] should ===(ContractKeyStreamRequest((), baseVal))
      }

      "errors on contractIdAtOffset presence" in { _ =>
        a[DeserializationException] shouldBe thrownBy {
          withSome.convertTo[T]
        }
        a[DeserializationException] shouldBe thrownBy {
          withNone.convertTo[T]
        }
      }
    }

    "resuming JSON reader" should {
      type T = ContractKeyStreamRequest[Option[Option[ContractId]], JsValue]

      "shares EnrichedContractKey format" in { _ =>
        JsObject(baseMap).convertTo[T] should ===(ContractKeyStreamRequest(None, baseVal))
      }

      "distinguishes null and string" in { _ =>
        withSome.convertTo[T] should ===(ContractKeyStreamRequest(Some(Some("hi")), baseVal))
        withNone.convertTo[T] should ===(ContractKeyStreamRequest(Some(None), baseVal))
      }
    }
  }
}
