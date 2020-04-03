// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.trigger.test

import akka.stream.scaladsl.{Flow}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.speedy.SExpr
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.testing.utils.{SuiteResourceManagementAroundAll}
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.{value => LedgerApi}
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest._
import scalaz.syntax.traverse._

import com.digitalasset.daml.lf.engine.trigger.TriggerMsg

abstract class AbstractFuncTests
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with SuiteResourceManagementAroundAll
    with TryValues {
  self: Suite =>

  this.getClass.getSimpleName can {
    "AcsTests" should {
      val assetId = LedgerApi.Identifier(packageId, "ACS", "Asset")
      val assetMirrorId = LedgerApi.Identifier(packageId, "ACS", "AssetMirror")
      def asset(party: String): CreateCommand =
        CreateCommand(
          templateId = Some(assetId),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(LedgerApi.RecordField("issuer", Some(LedgerApi.Value().withParty(party)))))))

      final case class AssetResult(
          successfulCompletions: Long,
          failedCompletions: Long,
          activeAssets: Set[String])

      def toResult(expr: SExpr): AssetResult = {
        val fields = expr.asInstanceOf[SEValue].v.asInstanceOf[SRecord].values
        AssetResult(
          successfulCompletions = fields.get(1).asInstanceOf[SInt64].value,
          failedCompletions = fields.get(2).asInstanceOf[SInt64].value,
          activeAssets = fields
            .get(0)
            .asInstanceOf[SList]
            .list
            .map(x =>
              x.asInstanceOf[SContractId].value.asInstanceOf[AbsoluteContractId].coid.toString)
            .toSet
        )
      }

      "1 create" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()
          // Start the future here
          finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(6))._2
          // Execute commands
          contractId <- create(client, party, asset(party))
          // Wait for the trigger to terminate
          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          assert(result.activeAssets == Seq(contractId).toSet)
          assert(result.successfulCompletions == 2)
          assert(result.failedCompletions == 0)
          assert(acs(assetMirrorId).size == 1)
        }
      }

      "2 creates" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()

          finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(12))._2

          contractId1 <- create(client, party, asset(party))
          contractId2 <- create(client, party, asset(party))

          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          assert(result.activeAssets == Seq(contractId1, contractId2).toSet)
          assert(result.successfulCompletions == 4)
          assert(result.failedCompletions == 0)
          assert(acs(assetMirrorId).size == 2)
        }
      }

      "2 creates and 2 archives" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()

          finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(16))._2

          contractId1 <- create(client, party, asset(party))
          contractId2 <- create(client, party, asset(party))
          _ <- archive(client, party, assetId, contractId1)
          _ <- archive(client, party, assetId, contractId2)

          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          assert(result.activeAssets == Seq().toSet)
          assert(result.successfulCompletions == 4)
          assert(result.failedCompletions == 0)
          assert(acs(assetMirrorId).size == 2)
        }
      }
    }

    "TimeTests" should {
      "test" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("Time:test"), party)
          (acs, offset) <- runner.queryACS()
          finalState <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(4))._2
        } yield {
          finalState match {
            case SEValue(SRecord(_, _, values)) if values.size == 2 =>
              values.get(1) match {
                case SList(items) if items.length == 2 =>
                  val t0 = items.slowApply(0).asInstanceOf[STimestamp].value
                  val t1 = items.slowApply(1).asInstanceOf[STimestamp].value
                  config.timeProviderType match {
                    case None => fail("No time provider type specified")
                    case Some(TimeProviderType.WallClock) =>
                      // Given the limited resolution it can happen that t0 == t1
                      assert(t0 >= t1)
                    case Some(TimeProviderType.Static) =>
                      assert(t0 == t1)
                  }
                case v => fail(s"Expected list with 2 elements but got $v")
              }
            case _ => fail(s"Expected Tuple2 but got $finalState")
          }
        }
      }
    }
  }
}
