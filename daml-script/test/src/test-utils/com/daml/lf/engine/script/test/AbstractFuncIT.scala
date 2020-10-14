// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref._
import com.daml.lf.data.{FrontStack, FrontStackCons, Numeric}
import com.daml.lf.engine.script.RunnerConfig
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import org.scalatest._
import spray.json.{JsNumber, JsObject, JsString}

abstract class AbstractFuncIT
    extends AsyncWordSpec
    with SandboxParticipantFixture
    with Matchers
    with SuiteResourceManagementAroundAll {
  val (stableDar, stableEnvIface) = readDar(stableDarFile)
  val (devDar, devDarEnvIface) = readDar(devDarFile)

  def assertSTimestamp(v: SValue) =
    v match {
      case STimestamp(t0) => t0
      case _ => fail(s"Expected STimestamp but got $v")
    }

  s"DAML Script func tests: ${timeMode}" can {
    "test0" should {
      "create two accepted proposals" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:test0"),
            dar = stableDar)
        } yield {
          assert(vals.size == 5)
          val alice = vals.get(0) match {
            case SParty(alice) => alice
            case v => fail(s"Expected SParty but got $v")
          }
          val bob = vals.get(1) match {
            case SParty(bob) => bob
            case v => fail(s"Expected SParty but got $v")
          }
          // allocateParty should return a fresh party
          assert(alice != bob)
          vals.get(2) match {
            case SList(
                FrontStackCons(
                  SRecord(_, _, t1),
                  FrontStackCons(SRecord(_, _, t2), FrontStack()))) =>
              t1 should contain theSameElementsInOrderAs (Seq(SParty(alice), SParty(bob)))
              t2 should contain theSameElementsInOrderAs (Seq(SParty(alice), SParty(bob)))
            case v => fail(s"Expected SList but got $v")
          }
          assert(vals.get(3) == SList(FrontStack.empty))
          vals.get(4) match {
            case SList(FrontStackCons(SRecord(_, _, vals), FrontStack())) =>
              vals should contain theSameElementsInOrderAs (Seq[SValue](SParty(alice), SInt64(42)))
            case v => fail(s"Expected a single SRecord but got $v")
          }
        }
      }
    }
    "test1" should {
      "handle numerics correctly" in {
        for {
          clients <- participantClients()
          v <- run(clients, QualifiedName.assertFromString("ScriptTest:test1"), dar = stableDar)
        } yield {
          assert(v == SNumeric(Numeric.assertFromString("2.12000000000")))
        }
      }
    }
    "test2" should {
      "extract value from input" in {
        for {
          clients <- participantClients()
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:test2"),
            dar = stableDar,
            inputValue = Some(JsObject(("p", JsString("Alice")), ("v", JsNumber(42)))))
        } yield {
          assert(v == SInt64(42))
        }
      }
    }
    "test3" should {
      "support submitMustFail" in {
        for {
          clients <- participantClients()
          v <- run(clients, QualifiedName.assertFromString("ScriptTest:test3"), dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
    "test4" should {
      "return new contract in query" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:test4"),
            dar = stableDar)
        } yield {
          assert(vals.size == 2)
          assert(vals.get(0) == vals.get(1))
        }
      }
    }
    "testKey" should {
      "support exerciseByKeyCmd" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testKey"),
            dar = stableDar)
        } yield {
          assert(vals.size == 2)
          assert(vals.get(0) == vals.get(1))
        }
      }
    }
    "testCreateAndExercise" should {
      "support createAndExerciseCmd" in {
        for {
          clients <- participantClients()
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testCreateAndExercise"),
            dar = stableDar)
        } yield {
          assert(v == SInt64(42))
        }
      }
    }
    "testGetTime" should {
      "not go backwards in time" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testGetTime"),
            dar = stableDar)
        } yield {
          assert(vals.size == 2)
          val t0 = assertSTimestamp(vals.get(0))
          val t1 = assertSTimestamp(vals.get(1))
          // Note that even in wallclock mode we cannot use strict inequality due to time
          // resolution (observed in CI)
          assert(t0 <= t1)
        }

      }
    }
    "testPartyIdHint" should {
      "allocate a party with the given hint" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:partyIdHintTest"),
            dar = stableDar)
        } yield {
          assert(vals.size == 2)
          assert(vals.get(0) == SParty(Party.assertFromString("carol")))
          assert(vals.get(1) == SParty(Party.assertFromString("dan")))
        }
      }
    }
    "testListKnownParties" should {
      "list newly allocated parties" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:listKnownPartiesTest"),
            dar = stableDar)
        } yield {
          assert(vals.size == 2)
          vals.get(0) match {
            case SList(FrontStackCons(SRecord(_, _, details), FrontStack())) =>
              details should contain theSameElementsInOrderAs (Seq(
                vals.get(1),
                SOptional(Some(SText("myparty"))),
                SBool(true)))
          }
        }
      }
    }
    "testStack" should {
      "not stackoverflow" in {
        for {
          clients <- participantClients()
          v <- run(clients, QualifiedName.assertFromString("ScriptTest:testStack"), dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
    "testMaxInboundMessageSize" should {
      "succeed despite large message" in {
        for {
          clients <- participantClients(
            maxInboundMessageSize = RunnerConfig.DefaultMaxInboundMessageSize * 10)
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testMaxInboundMessageSize"),
            dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
    "ScriptExample" should {
      "succeed" in {
        for {
          clients <- participantClients()
          v <- run(clients, QualifiedName.assertFromString("ScriptExample:test"), dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
    "testQueryContractId" should {
      "support queryContractId" in {
        for {
          clients <- participantClients()
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testQueryContractId"),
            dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
    "testQueryContractKey" should {
      "support queryContractKey" in {
        for {
          clients <- participantClients()
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testQueryContractKey"),
            dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
    "traceOrder" should {
      "emit trace statements in correct order" in {
        def traceMsg(msg: String) = s"""[DA.Internal.Prelude:540]: "$msg""""
        for {
          clients <- participantClients()
          _ = LogCollector.clear()
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:traceOrder"),
            dar = stableDar)
        } yield {
          assert(v == SUnit)
          val logMsgs = LogCollector.events.map(_.getMessage)
          assert(logMsgs == Seq(traceMsg("abc"), traceMsg("def"), traceMsg("abc"), traceMsg("def")))
        }
      }
    }
    "testContractId" should {
      "convert ContractId to Text" in {
        for {
          clients <- participantClients()
          SRecord(_, _, vals) <- run(
            clients,
            QualifiedName.assertFromString("TestContractId:testContractId"),
            dar = devDar)
        } yield {
          assert(vals.size == 2)
          (vals.get(0), vals.get(1)) match {
            case (SContractId(cid), SText(t)) =>
              assert(cid.coid == t)
            case (v0, v1) => fail(s"Expected SContractId, SText but got $v0, $v1")
          }
        }
      }
    }
    "testMultiPartyQuery" should {
      "should return contracts for all listed parties" in {
        for {
          clients <- participantClients()
          v <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:testMultiPartyQueries"),
            dar = stableDar)
        } yield {
          assert(v == SUnit)
        }
      }
    }
  }
}
