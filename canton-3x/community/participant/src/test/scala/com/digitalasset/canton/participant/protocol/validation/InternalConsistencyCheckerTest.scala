// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.{
  ErrorWithInternalConsistencyCheck,
  checkRollbackScopeOrder,
}
import com.digitalasset.canton.protocol.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.util.Random

class InternalConsistencyCheckerTest extends AnyWordSpec with BaseTest {

  implicit val ec: ExecutionContext = directExecutionContext

  private val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  private def check(
      icc: InternalConsistencyChecker,
      views: Seq[FullTransactionViewTree],
  ): Either[ErrorWithInternalConsistencyCheck, Unit] = {
    icc.check(NonEmptyUtil.fromUnsafe(views), _ => false)
  }

  "Rollback scope ordering" when {

    "checkRollbackScopeOrder should validate sequences of scopes" in {
      val ops: Seq[RollbackContext => RollbackContext] = Seq(
        _.enterRollback,
        _.enterRollback,
        _.exitRollback,
        _.enterRollback,
        _.exitRollback,
        _.exitRollback,
        _.enterRollback,
        _.exitRollback,
      )

      val (_, testScopes) = ops.foldLeft((RollbackContext.empty, Seq(RollbackContext.empty)))({
        case ((c, seq), op) =>
          val nc = op(c)
          (nc, seq :+ nc)
      })

      Random.shuffle(testScopes).sorted shouldBe testScopes
      checkRollbackScopeOrder(testScopes) shouldBe Right(())
      checkRollbackScopeOrder(testScopes.reverse).isLeft shouldBe true
    }
  }

  "Internal consistency checker" when {

    val relevantExamples = factory.standardHappyCases.filter(_.rootTransactionViewTrees.nonEmpty)

    forEvery(relevantExamples) { example =>
      s"checking $example" must {

        val sut = new InternalConsistencyChecker(testedProtocolVersion, loggerFactory)

        "yield the correct result" in {
          check(sut, example.rootTransactionViewTrees).isRight shouldBe true
        }

        "reinterpret views individually" in {
          example.transactionViewTrees.foreach { viewTree =>
            check(sut, Seq(viewTree)) shouldBe Right(())
          }
        }
      }
    }

  }
}
