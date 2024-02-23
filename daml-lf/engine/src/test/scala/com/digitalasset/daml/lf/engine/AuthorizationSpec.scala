// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
import com.daml.lf.ledger.Authorize
import com.daml.lf.ledger.FailedAuthorization._
import com.daml.lf.speedy.DefaultAuthorizationChecker
import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, Node}
import com.daml.lf.transaction.test.{TestIdFactory, TestNodeBuilder, TransactionBuilder}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ValueRecord
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside

class AuthorizationSpec extends AnyFreeSpec with Matchers with Inside with TestIdFactory {

  // Test the various forms of FailedAuthorization which can be returned from CheckAuthorization
  // for the 4 kinds of GenActionNode: Create/Fetch/Lookup/Exercise.

  import TransactionBuilder.Implicits._

  private val builder = TestNodeBuilder

  private def makeCreateNode(
      signatories: Seq[Party] = Seq("Alice", "Bob"),
      maintainers: Set[Party] = Seq("Alice"),
  ): Node.Create = {

    val templateId = "M:T"
    val init = builder
      .create(
        id = newCid,
        templateId = templateId,
        argument = ValueRecord(None, ImmArray.Empty),
        signatories = signatories,
        observers = Seq("Carl"),
        key = CreateKey.NoKey,
      )
    init.copy(
      // By default maintainers are added to signatories so do this to allow error case testing
      keyOpt = Some(
        GlobalKeyWithMaintainers.assertBuild(
          templateId,
          Value.ValueUnit,
          maintainers,
          init.packageName,
        )
      )
    )
  }

  "create" - {
    // TEST_EVIDENCE: Authorization: well-authorized create is accepted
    "ok" in {
      val createNode = makeCreateNode()
      val auth = Authorize(Set("Alice", "Bob", "Mary"))
      val fails = DefaultAuthorizationChecker.authorizeCreate(optLocation = None, createNode)(auth)
      fails shouldBe Nil
    }
    // TEST_EVIDENCE: Authorization: create with no signatories is rejected
    "NoSignatories" in {
      val createNode = makeCreateNode(signatories = Nil, maintainers = Nil)
      val auth = Authorize(Set("Alice", "Bob", "Mary"))
      val fails = DefaultAuthorizationChecker.authorizeCreate(optLocation = None, createNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case _: NoSignatories =>
        }
      }
    }
    // TEST_EVIDENCE: Authorization: badly-authorized create is rejected
    "CreateMissingAuthorization" in {
      val createNode = makeCreateNode()
      val auth = Authorize(Set("Alice"))
      val fails = DefaultAuthorizationChecker.authorizeCreate(optLocation = None, createNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case x: CreateMissingAuthorization =>
          x.authorizingParties shouldBe Set("Alice")
          x.requiredParties shouldBe Set("Alice", "Bob")
        }
      }
    }
    // TEST_EVIDENCE: Authorization: create with non-signatory maintainers is rejected
    "MaintainersNotSubsetOfSignatories" in {
      val createNode = makeCreateNode(maintainers = Seq("Alice", "Mary"))
      val auth = Authorize(Set("Alice", "Bob", "Mary"))
      val fails = DefaultAuthorizationChecker.authorizeCreate(optLocation = None, createNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case x: MaintainersNotSubsetOfSignatories =>
          x.signatories shouldBe Set("Alice", "Bob")
          x.maintainers shouldBe Set("Alice", "Mary")
        }
      }
    }
  }

  "fetch" - {
    val contract = makeCreateNode()
    val fetchNode = TestNodeBuilder.fetch(contract, byKey = false)
    // TEST_EVIDENCE: Authorization: well-authorized fetch is accepted
    "ok" in {
      val auth = Authorize(Set("Alice", "Mary", "Nigel"))
      val fails = DefaultAuthorizationChecker.authorizeFetch(optLocation = None, fetchNode)(auth)
      fails shouldBe Nil
    }
    // TEST_EVIDENCE: Authorization: badly-authorized fetch is rejected
    "FetchMissingAuthorization" in {
      val auth = Authorize(Set("Mary", "Nigel"))
      val fails = DefaultAuthorizationChecker.authorizeFetch(optLocation = None, fetchNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case x: FetchMissingAuthorization =>
          x.stakeholders shouldBe Set("Alice", "Bob", "Carl")
          x.authorizingParties shouldBe Set("Mary", "Nigel")
        }
      }
    }
  }

  "lookup-by-key" - {
    val contract = makeCreateNode(maintainers = Seq("Alice", "Bob"))
    val lookupNode = builder.lookupByKey(contract)
    // TEST_EVIDENCE: Authorization: well-authorized lookup is accepted
    "ok" in {
      val auth = Authorize(Set("Alice", "Bob", "Mary"))
      val fails =
        DefaultAuthorizationChecker.authorizeLookupByKey(optLocation = None, lookupNode)(auth)
      fails shouldBe Nil
    }
    // TEST_EVIDENCE: Authorization: badly-authorized lookup is rejected
    "LookupByKeyMissingAuthorization" in {
      val auth = Authorize(Set("Alice", "Mary"))
      val fails =
        DefaultAuthorizationChecker.authorizeLookupByKey(optLocation = None, lookupNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case x: LookupByKeyMissingAuthorization =>
          x.maintainers shouldBe Set("Alice", "Bob")
          x.authorizingParties shouldBe Set("Alice", "Mary")
        }
      }
    }
  }

  "exercise" - {
    val contract = makeCreateNode(maintainers = Seq("Alice", "Bob"))
    def makeExeNode(actingParties: Set[Party] = Seq("Alice", "Mary")) = {
      builder.exercise(
        contract = contract,
        choice = "C",
        consuming = true,
        actingParties = actingParties,
        argument = ValueRecord(None, ImmArray.empty),
        byKey = false,
      )
    }
    // TEST_EVIDENCE: Authorization: well-authorized exercise is accepted
    "ok" in {
      val auth = Authorize(Set("Alice", "John", "Mary"))
      val exeNode = makeExeNode()
      val fails = DefaultAuthorizationChecker.authorizeExercise(optLocation = None, exeNode)(auth)
      fails shouldBe Nil
    }
    // TEST_EVIDENCE: Authorization: exercise with no controllers is rejected
    "NoControllers" in {
      val exeNode = makeExeNode(actingParties = Nil)
      val auth = Authorize(Set("Alice", "John", "Mary"))
      val fails = DefaultAuthorizationChecker.authorizeExercise(optLocation = None, exeNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case _: NoControllers =>
        }
      }
    }
    // TEST_EVIDENCE: Authorization: badly-authorized exercise is rejected
    "ExerciseMissingAuthorization" in {
      val exeNode = makeExeNode()
      val auth = Authorize(Set("Alice", "John"))
      val fails = DefaultAuthorizationChecker.authorizeExercise(optLocation = None, exeNode)(auth)
      inside(fails) { case List(oneFail) =>
        inside(oneFail) { case x: ExerciseMissingAuthorization =>
          x.requiredParties shouldBe Set("Alice", "Mary")
          x.authorizingParties shouldBe Set("Alice", "John")
        }
      }
    }
  }
}
