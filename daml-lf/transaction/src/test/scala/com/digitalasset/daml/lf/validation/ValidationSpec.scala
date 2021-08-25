// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, TypeConName, ChoiceName, Party}
import com.daml.lf.transaction.Node.{
  KeyWithMaintainers,
  GenNode,
  NodeCreate,
  NodeFetch,
  NodeLookupByKey,
  NodeExercises,
}
import com.daml.lf.value.{Value => V}

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.HashMap

class ValidationSpec extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {
  //--[Tweaks]--
  //
  // A 'Tweak[X]' is a family of (small) modifications to a value of type X.
  //
  // This test file constructs tweaks for 'VersionedTransaction' (VTX).
  // All tweaks are SIGNIFICANT since 'isReplayedBy' is simply structual equality
  //
  // We aim to tweak every field of every ActionNode in a TX.
  //
  // The tweaks are tested by running over a hand constructed list of 'preTweakedVTXs'. We
  // are careful to limit the combinational explosion to just what is necessary to ensure
  // every tweak under test is hit by at least one pre-tweaked node.
  //
  // The testcases are organised so failure is detected and reported for a named tweak.

  private class Tweak[X](val run: X => List[X])

  private object Tweak {
    def single[X](f1: PartialFunction[X, X]): Tweak[X] = {
      apply(f1.andThen(List(_)))
    }
    def apply[X](f: PartialFunction[X, List[X]]): Tweak[X] = {
      new Tweak(f.orElse { case _ => List.empty })
    }
  }

  //--[types]--

  private type Val = V[V.ContractId]
  private type KWM = KeyWithMaintainers[Val]
  private type OKWM = Option[KWM]
  private type Exe = NodeExercises[NodeId, V.ContractId]
  private type Node = GenNode[NodeId, V.ContractId]
  private type VTX = VersionedTransaction[NodeId, V.ContractId]

  //--[samples]--

  private val samBool1 = true
  private val samBool2 = false

  private val samText1 = "some text"

  private val samContractId1 = V.ContractId.V1(crypto.Hash.hashPrivateKey("cid1"))
  private val samContractId2 = V.ContractId.V1(crypto.Hash.hashPrivateKey("cid2"))

  private val samTemplateId1 = Identifier.assertFromString("-samPkg-:SamModule:samName1")
  private val samTemplateId2 = Identifier.assertFromString("-samPkg-:SamModule:samName2")

  private val samChoiceName1 = ChoiceName.assertFromString("Choice1")
  private val samChoiceName2 = ChoiceName.assertFromString("Choice2")

  private val samParty1 = Party.assertFromString("party1")
  private val samParty2 = Party.assertFromString("party2")
  private val samPartyX = Party.assertFromString("partyX")

  private val samParties1: Set[Party] = Set()
  private val samParties2: Set[Party] = Set(samParty1)
  private val samParties3: Set[Party] = Set(samParty2)
  private val samParties4: Set[Party] = Set(samParty1, samParty2)

  private val samValue1: Val = V.ValueUnit
  private val samValue2: Val = V.ValueContractId(samContractId1)

  private val samKWM1 = KeyWithMaintainers(samValue1, samParties1)
  private val samKWM2 = KeyWithMaintainers(samValue1, samParties2)
  private val samKWM3 = KeyWithMaintainers(samValue2, samParties1)

  private val samVersion1: TransactionVersion = TransactionVersion.minVersion
  private val samVersion2: TransactionVersion = TransactionVersion.maxVersion

  private val someCreates: Seq[Node] =
    for {
      version <- Seq(samVersion1, samVersion2)
      key <- Seq(None, Some(samKWM1))
    } yield NodeCreate(
      coid = samContractId1,
      templateId = samTemplateId1,
      arg = samValue1,
      agreementText = samText1,
      signatories = samParties1,
      stakeholders = samParties2,
      key = key,
      version = version,
    )

  private val someFetches: Seq[Node] =
    for {
      version <- Seq(samVersion1, samVersion2)
      key <- Seq(None, Some(samKWM2))
      actingParties <- Seq(Set[Party](), Set(samParty1))
    } yield NodeFetch(
      coid = samContractId1,
      templateId = samTemplateId1,
      actingParties = actingParties,
      signatories = samParties2,
      stakeholders = samParties3,
      key = key,
      byKey = samBool1,
      version = version,
    )

  private val someLookups: Seq[Node] =
    for {
      version <- Seq(samVersion1, samVersion2)
      result <- Seq(None, Some(samContractId1))
    } yield NodeLookupByKey(
      templateId = samTemplateId1,
      result = result,
      key = samKWM3,
      version = version,
    )

  private val someExercises: Seq[Exe] =
    for {
      version <- Seq(samVersion1, samVersion2)
      key <- Seq(None, Some(samKWM1))
      exerciseResult <- Seq(None, Some(samValue2))
    } yield NodeExercises(
      targetCoid = samContractId2,
      templateId = samTemplateId2,
      choiceId = samChoiceName1,
      consuming = samBool1,
      actingParties = samParties1,
      chosenValue = samValue1,
      stakeholders = samParties2,
      signatories = samParties3,
      choiceObservers = samParties4,
      children = ImmArray.empty,
      exerciseResult = exerciseResult,
      key = key,
      byKey = samBool2,
      version = version,
    )

  //--[running tweaks]--
  // We dont aim for much coverage in the overal TX shape; we limit to either 0 or 1 level of nesting.

  private def flatVTXs: Seq[VTX] =
    (someCreates ++ someFetches ++ someLookups ++ someExercises).map { node =>
      val nid = NodeId(0)
      val version = TransactionVersion.minExceptions
      VersionedTransaction(version, HashMap(nid -> node), ImmArray(nid))
    }

  private def nestedVTXs: Seq[VTX] =
    for {
      exe <- someExercises
      child <- someExercises ++ someCreates ++ someLookups ++ someFetches
    } yield {
      val nid0 = NodeId(0)
      val nid1 = NodeId(1)
      val parent = exe.copy(children = ImmArray(nid1))
      val version = TransactionVersion.minExceptions
      VersionedTransaction(
        version,
        HashMap(nid0 -> parent, nid1 -> child),
        ImmArray(nid0),
      )
    }

  private def preTweakedVTXs: Seq[VTX] = {
    // we ensure the preTweaked txs are properly normalized.
    (flatVTXs ++ nestedVTXs).map(Normalization.normalizeTx)
  }

  private def runTweak(tweak: Tweak[VTX]): Seq[(VTX, VTX)] =
    for {
      txA <- preTweakedVTXs
      txB <- tweak.run(txA)
    } yield (txA, txB)

  //--[changes]--
  // Change functions must never be identity.

  private def changeBoolean(x: Boolean) = { !x }

  private def changeContractId(x: V.ContractId): V.ContractId = {
    if (x != samContractId1) samContractId1 else samContractId2
  }

  private def changeTemplateId(x: TypeConName): TypeConName = {
    if (x != samTemplateId1) samTemplateId1 else samTemplateId2
  }

  private def changeChoiceId(x: ChoiceName): ChoiceName = {
    if (x != samChoiceName1) samChoiceName1 else samChoiceName2
  }

  private def changeValue(x: Val): Val = {
    if (x != samValue1) samValue1 else samValue2
  }

  private def changeVersion(x: TransactionVersion): TransactionVersion = {
    if (x != samVersion1) samVersion1 else samVersion2
  }

  private def changeText(x: String): String = {
    x + "_XXX"
  }

  //--[predicates]--
  // Some tweaks have version dependant significance.

  private def versionSinceMinByKey(v: TransactionVersion): Boolean = {
    import scala.Ordering.Implicits.infixOrderingOps
    v >= TransactionVersion.minByKey
  }

  //--[shared sub tweaks]--

  private val tweakPartySet = Tweak[Set[Party]] { case xs =>
    (xs + samPartyX) ::
      List(samParties1, samParties2, samParties3, samParties4).filter(set => set != xs)
  }

  private val tweakKeyMaintainers = Tweak[KWM] { case x =>
    List(samKWM1, samKWM2, samKWM3).filter(y => x != y)
  }

  private val tweakOptKeyMaintainers = Tweak[OKWM] {
    case None => List(Some(samKWM1), Some(samKWM2), Some(samKWM3))
    case Some(x) => None :: List(samKWM1, samKWM2, samKWM3).filter(y => x != y).map(Some(_))
  }

  private val tweakOptContractId = Tweak[Option[V.ContractId]] { case x =>
    List(None, Some(samContractId1), Some(samContractId2)).filter(y => x != y)
  }

  //--[Create node tweaks]--

  private val tweakCreateCoid = Tweak.single[Node] { case nc: Node.NodeCreate[_] =>
    nc.copy(coid = changeContractId(nc.coid))
  }
  private val tweakCreateTemplateId = Tweak.single[Node] { case nc: Node.NodeCreate[_] =>
    nc.copy(templateId = changeTemplateId(nc.templateId))
  }
  private val tweakCreateArg = Tweak.single[Node] { case nc: Node.NodeCreate[_] =>
    nc.copy(arg = changeValue(nc.arg))
  }
  private val tweakCreateAgreementText = Tweak.single[Node] { case nc: Node.NodeCreate[_] =>
    nc.copy(agreementText = changeText(nc.agreementText))
  }
  private val tweakCreateSignatories = Tweak[Node] { case nc: Node.NodeCreate[_] =>
    tweakPartySet.run(nc.signatories).map { x => nc.copy(signatories = x) }
  }
  private val tweakCreateStakeholders = Tweak[Node] { case nc: Node.NodeCreate[_] =>
    tweakPartySet.run(nc.stakeholders).map { x => nc.copy(stakeholders = x) }
  }
  private def tweakCreateKey(tweakOptKeyMaintainers: Tweak[OKWM]) = Tweak[Node] {
    case nc: Node.NodeCreate[_] =>
      tweakOptKeyMaintainers.run(nc.key).map { x => nc.copy(key = x) }
  }
  private val tweakCreateVersion = Tweak.single[Node] { case nc: Node.NodeCreate[_] =>
    nc.copy(version = changeVersion(nc.version))
  }

  private val sigCreateTweaks =
    Map(
      "tweakCreateCoid" -> tweakCreateCoid,
      "tweakCreateTemplateId" -> tweakCreateTemplateId,
      "tweakCreateArg" -> tweakCreateArg,
      "tweakCreateAgreementText" -> tweakCreateAgreementText,
      "tweakCreateSignatories" -> tweakCreateSignatories,
      "tweakCreateStakeholders" -> tweakCreateStakeholders,
      "tweakCreateKey" -> tweakCreateKey(tweakOptKeyMaintainers),
      "tweakCreateVersion" -> tweakCreateVersion,
    )

  //--[Fetch node tweaks]--

  private val tweakFetchCoid = Tweak.single[Node] { case nf: Node.NodeFetch[_] =>
    nf.copy(coid = changeContractId(nf.coid))
  }
  private val tweakFetchTemplateId = Tweak.single[Node] { case nf: Node.NodeFetch[_] =>
    nf.copy(templateId = changeTemplateId(nf.templateId))
  }
  private val tweakFetchActingPartiesNonEmpty = Tweak[Node] { case nf: Node.NodeFetch[_] =>
    tweakPartySet.run(nf.actingParties).map { x => nf.copy(actingParties = x) }
  }
  private val tweakFetchSignatories = Tweak[Node] { case nf: Node.NodeFetch[_] =>
    tweakPartySet.run(nf.signatories).map { x => nf.copy(signatories = x) }
  }
  private val tweakFetchStakeholders = Tweak[Node] { case nf: Node.NodeFetch[_] =>
    tweakPartySet.run(nf.stakeholders).map { x => nf.copy(stakeholders = x) }
  }
  private def tweakFetchKey(tweakOptKeyMaintainers: Tweak[OKWM]) = Tweak[Node] {
    case nf: Node.NodeFetch[_] =>
      tweakOptKeyMaintainers.run(nf.key).map { x => nf.copy(key = x) }
  }
  private def tweakFetchByKey(whenVersion: TransactionVersion => Boolean) = Tweak.single[Node] {
    case nf: Node.NodeFetch[_] if whenVersion(nf.version) =>
      nf.copy(byKey = changeBoolean(nf.byKey))
  }
  private val tweakFetchVersion = Tweak.single[Node] { case nf: Node.NodeFetch[_] =>
    nf.copy(version = changeVersion(nf.version))
  }

  private val sigFetchTweaks =
    Map(
      "tweakFetchCoid" -> tweakFetchCoid,
      "tweakFetchTemplateId" -> tweakFetchTemplateId,
      "tweakFetchActingParties" -> tweakFetchActingPartiesNonEmpty,
      "tweakFetchSignatories" -> tweakFetchSignatories,
      "tweakFetchStakeholders" -> tweakFetchStakeholders,
      "tweakFetchKey" -> tweakFetchKey(tweakOptKeyMaintainers),
      "tweakFetchByKey(New Version)" -> tweakFetchByKey(versionSinceMinByKey),
      "tweakFetchVersion" -> tweakFetchVersion,
    )

  //--[LookupByKey node tweaks]--

  private val tweakLookupTemplateId = Tweak.single[Node] { case nl: Node.NodeLookupByKey[_] =>
    nl.copy(templateId = changeTemplateId(nl.templateId))
  }
  private val tweakLookupKey = Tweak[Node] { case nl: Node.NodeLookupByKey[_] =>
    tweakKeyMaintainers.run(nl.key).map { x => nl.copy(key = x) }
  }
  private val tweakLookupResult = Tweak[Node] { case nl: Node.NodeLookupByKey[_] =>
    tweakOptContractId.run(nl.result).map { x => nl.copy(result = x) }
  }
  private val tweakLookupVersion = Tweak.single[Node] { case nl: Node.NodeLookupByKey[_] =>
    nl.copy(version = changeVersion(nl.version))
  }

  private val sigLookupTweaks =
    Map(
      "tweakLookupTemplateId" -> tweakLookupTemplateId,
      "tweakLookupKey" -> tweakLookupKey,
      "tweakLookupResult" -> tweakLookupResult,
      "tweakLookupVersion" -> tweakLookupVersion,
    )

  //--[Exercise node tweaks]--

  private val tweakExerciseTargetCoid = Tweak.single[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.copy(targetCoid = changeContractId(ne.targetCoid))
  }
  private val tweakExerciseTemplateId = Tweak.single[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.copy(templateId = changeTemplateId(ne.templateId))
  }
  private val tweakExerciseChoiceId = Tweak.single[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.copy(choiceId = changeChoiceId(ne.choiceId))
  }
  private val tweakExerciseConsuming = Tweak.single[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.copy(consuming = changeBoolean(ne.consuming))
  }
  private val tweakExerciseActingParties = Tweak[Node] { case ne: Node.NodeExercises[_, _] =>
    tweakPartySet.run(ne.actingParties).map { x => ne.copy(actingParties = x) }
  }
  private val tweakExerciseChosenValue = Tweak.single[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.copy(chosenValue = changeValue(ne.chosenValue))
  }
  private val tweakExerciseStakeholders = Tweak[Node] { case ne: Node.NodeExercises[_, _] =>
    tweakPartySet.run(ne.stakeholders).map { x => ne.copy(stakeholders = x) }
  }
  private val tweakExerciseSignatories = Tweak[Node] { case ne: Node.NodeExercises[_, _] =>
    tweakPartySet.run(ne.signatories).map { x => ne.copy(signatories = x) }
  }
  private val tweakExerciseChoiceObservers = Tweak[Node] { case ne: Node.NodeExercises[_, _] =>
    tweakPartySet.run(ne.choiceObservers).map { x => ne.copy(choiceObservers = x) }
  }
  private val tweakExerciseExerciseResult = Tweak[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.exerciseResult match {
      case None => List(ne.copy(exerciseResult = Some(samValue1)))
      case Some(v) =>
        List(
          ne.copy(exerciseResult = Some(changeValue(v))),
          ne.copy(exerciseResult = None),
        )
    }
  }

  private def tweakExerciseKey(tweakOptKeyMaintainers: Tweak[OKWM]) = Tweak[Node] {
    case ne: Node.NodeExercises[_, _] =>
      tweakOptKeyMaintainers.run(ne.key).map { x => ne.copy(key = x) }
  }
  private def tweakExerciseByKey(whenVersion: TransactionVersion => Boolean) = Tweak.single[Node] {
    case ne: Node.NodeExercises[_, _] if whenVersion(ne.version) =>
      ne.copy(byKey = changeBoolean(ne.byKey))
  }
  private val tweakExerciseVersion = Tweak.single[Node] { case ne: Node.NodeExercises[_, _] =>
    ne.copy(version = changeVersion(ne.version))
  }

  private val sigExeTweaks =
    Map(
      "tweakExerciseTargetCoid" -> tweakExerciseTargetCoid,
      "tweakExerciseTemplateId" -> tweakExerciseTemplateId,
      "tweakExerciseChoiceId" -> tweakExerciseChoiceId,
      "tweakExerciseConsuming" -> tweakExerciseConsuming,
      "tweakExerciseActingParties" -> tweakExerciseActingParties,
      "tweakExerciseChosenValue" -> tweakExerciseChosenValue,
      "tweakExerciseStakeholders" -> tweakExerciseStakeholders,
      "tweakExerciseSignatories" -> tweakExerciseSignatories,
      "tweakExerciseChoiceObservers" -> tweakExerciseChoiceObservers,
      "tweakExerciseExerciseResult" -> tweakExerciseExerciseResult,
      "tweakExerciseKey" -> tweakExerciseKey(tweakOptKeyMaintainers),
      "tweakExerciseByKey(New Version)" -> tweakExerciseByKey(versionSinceMinByKey),
      "tweakExerciseVersion" -> tweakExerciseVersion,
    )

  //--[significant tx tweaks]--

  private def tweakTxNodes(tweakNode: Tweak[Node]) = Tweak[VTX] { vtx =>
    // tweak any node in a transaction
    vtx.transaction match {
      case GenTransaction(nodeMapA, roots) =>
        for {
          nid <- nodeMapA.keys.toList
          nodeB <- tweakNode.run(nodeMapA(nid))
        } yield {
          val nodeMapB = nodeMapA + (nid -> nodeB)
          VersionedTransaction(vtx.version, nodeMapB, roots)
        }
    }
  }

  private def significantTweaks: Map[String, Tweak[VTX]] = {
    (sigCreateTweaks ++ sigFetchTweaks ++ sigLookupTweaks ++ sigExeTweaks)
      .map { case (name, tw) => (name, tweakTxNodes(tw)) }
  }

  //--[per tweak tests]--

  "Significant tweaks" - {
    significantTweaks.foreach { case (name, tweak) =>
      val pairs = runTweak(tweak)
      val n = pairs.length
      assert(n > 0) // ensure tweak actualy applies to something
      s"[#$n] $name" in {
        val testCases = Table[VTX, VTX](("txA", "txB"), pairs: _*)
        forEvery(testCases) { case (txA, txB) =>
          Validation.isReplayedBy(txA, txB) shouldBe a[Left[_, _]]
        }
      }
    }
  }

}
