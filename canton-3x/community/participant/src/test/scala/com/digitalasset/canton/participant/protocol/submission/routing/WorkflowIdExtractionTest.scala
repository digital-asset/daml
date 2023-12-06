// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, DomainAlias, LfWorkflowId}
import org.scalatest.wordspec.AnyWordSpec

class WorkflowIdExtractionTest extends AnyWordSpec with BaseTest {
  val domainId = DomainId(UniqueIdentifier.tryCreate("domain", "id"))

  "toDomainId" should {

    "not extract anything if workflow id is empty" in {
      TransactionData.toDomainId(
        maybeWorkflowId = None,
        domainIdResolver = _ => None,
      ) shouldBe Right(None)
    }

    "extract domain-id correctly" in {
      val workflowId = LfWorkflowId.assertFromString(s"domain-id:${domainId.toProtoPrimitive}")
      TransactionData.toDomainId(
        maybeWorkflowId = Some(workflowId),
        domainIdResolver = _ => None,
      ) shouldBe Right(Some(domainId))
    }

    "extract domain-id correctly with prefix content" in {
      val workflowId = LfWorkflowId.assertFromString(
        s"some prefixed content domain-id:${domainId.toProtoPrimitive}"
      )
      TransactionData.toDomainId(
        maybeWorkflowId = Some(workflowId),
        domainIdResolver = _ => None,
      ) shouldBe Right(Some(domainId))
    }

    "fail if domainId is invalid" in {
      val workflowId =
        LfWorkflowId.assertFromString(s"some prefixed content domain-id:some invalid DomainId")
      TransactionData.toDomainId(
        maybeWorkflowId = Some(workflowId),
        domainIdResolver = _ => None,
      ) match {
        case Left(_: TransactionRoutingError.MalformedInputErrors.InvalidDomainId.Error) => succeed
        case Left(error) => fail(s"incorrect error: $error")
        case _ => fail("result should be a failure")
      }
    }

    "extract domain-id from a valid domain alias" in {
      val domainAlias = DomainAlias.tryCreate("domainAlias")
      val workflowId = LfWorkflowId.assertFromString(s"${domainAlias.toProtoPrimitive}")
      TransactionData.toDomainId(
        maybeWorkflowId = Some(workflowId),
        domainIdResolver = {
          case `domainAlias` => Some(domainId)
          case _ => fail("should be called with the extracted domain alias")
        },
      ) shouldBe Right(Some(domainId))
    }

    "not extract domain-id from a valid domain alias if it cannot be looked up" in {
      val domainAlias = DomainAlias.tryCreate("domainAlias")
      val workflowId = LfWorkflowId.assertFromString(s"${domainAlias.toProtoPrimitive}")
      TransactionData.toDomainId(
        maybeWorkflowId = Some(workflowId),
        domainIdResolver = {
          case `domainAlias` => None
          case _ => fail("should be called with the extracted domain alias")
        },
      ) shouldBe Right(None)
    }

    "fail for invalid domain alias" in {
      // LfWorkflowId is a ledger string, and every ledger string is a valid domain alias at the moment
      val workflowId = List.fill(300)("a").mkString.asInstanceOf[LfWorkflowId]
      TransactionData.toDomainId(
        maybeWorkflowId = Some(workflowId),
        domainIdResolver = _ => None,
      ) match {
        case Left(error: TransactionRoutingError.MalformedInputErrors.InvalidDomainAlias.Error) =>
          succeed
        case Left(error) => fail(s"incorrect error: $error")
        case _ => fail("result should be a failure")
      }
    }

  }
}
