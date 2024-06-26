// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.StateT
import cats.syntax.traverse.*
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.test.ValueGenerators
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder.*
import org.scalacheck.cats.implicits.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class LfGenerator extends AnyWordSpec with BaseTest with ScalaCheckDrivenPropertyChecks {

  implicit lazy val txArbitrary: Arbitrary[LfTransaction] = Arbitrary(LfGenerator.transactionGen(4))

  "LfGenerator" must {
    "produce well-formed transactions" in {
      forAll { (tx: LfTransaction) =>
        tx.isWellFormed shouldBe Set.empty
      }
    }
  }
}

/** Unlike the generators from the daml-lf repository, the transaction generator below creates well-formed
  * (in the sense of GenTransaction#isWellFormed) transactions
  */
object LfGenerator {
  val lfGen = ValueGenerators

  /** The LF generator generates huge (template) identifiers by default; truncate to something reasonable */
  def truncateIdentifier(id: Ref.Identifier): Ref.Identifier = {
    def truncateDotted(name: Ref.DottedName): Ref.DottedName =
      Ref.DottedName.assertFromNames(
        name.segments.relaxedSlice(0, 2).map(name => Ref.Name.assertFromString(name.take(10)))
      )
    id.copy(
      packageId = Ref.PackageId.assertFromString(id.packageId.take(10)),
      qualifiedName = Ref.QualifiedName.assertFromString(
        truncateDotted(id.qualifiedName.module).dottedName + ":" + truncateDotted(
          id.qualifiedName.name
        ).dottedName
      ),
    )
  }

  def createGen[N >: LfNodeCreate]: StateT[Gen, NodeIdState, LfAction] =
    for {
      node <- StateT.liftF(lfGen.malformedCreateNodeGen())
      fixedNode = node.copy(templateId = truncateIdentifier(node.coinst.template), keyOpt = None)
      action <- createFromLf[Gen](fixedNode)
    } yield action

  def fetchGen[N >: LfNodeFetch]: StateT[Gen, NodeIdState, LfAction] =
    for {
      node <- StateT.liftF(Gen.resize(10, lfGen.fetchNodeGen))
      templateId = truncateIdentifier(node.templateId)
      fixedNode = node.copy(templateId = templateId, keyOpt = None)
      action <- fetchFromLf[Gen](fixedNode)
    } yield action

  def exerciseGen(maxBranching: Int): StateT[Gen, NodeIdState, LfAction] =
    for {
      children <- generateMultiple(maxBranching)
      node <- StateT.liftF(Gen.resize(10, lfGen.danglingRefExerciseNodeGen))
      templateId = truncateIdentifier(node.templateId)
      fixedNode = node.copy(children = ImmArray.empty, templateId = templateId, keyOpt = None)
      action <- exerciseFromLf[Gen](fixedNode, children)
    } yield action

  def generateMultiple(maxBranching: Int): StateT[Gen, NodeIdState, List[LfAction]] =
    for {
      generators <- StateT.liftF {
        // Laziness prevents the circular dependency on exerciseGen from creating an infinite recursion
        Gen.lzy(
          Gen.resize(
            maxBranching,
            Gen.containerOf[List, StateT[Gen, NodeIdState, LfAction]](
              Gen.oneOf(
                createGen[LfNode],
                fetchGen[LfNode],
                exerciseGen(maxBranching),
              )
            ),
          )
        )
      }
      idAndMapList <- generators.sequence
    } yield idAndMapList

  def transactionGen(maxBranching: Int): Gen[LfTransaction] =
    toTransaction(generateMultiple(maxBranching))

}
