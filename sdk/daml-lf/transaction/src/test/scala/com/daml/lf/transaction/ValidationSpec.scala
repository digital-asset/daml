// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{
  Party,
  Identifier,
  PackageName,
  PackageVersion,
  TypeConName,
  ChoiceName,
}
import com.daml.lf.value.{Value => V}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.HashMap

class ValidationSpec extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {
  // --[Tweaks]--
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

  import Ordering.Implicits._

  private class Tweak[X](val run: X => List[X])

  private object Tweak {
    def single[X](f1: PartialFunction[X, X]): Tweak[X] = {
      apply(f1.andThen(List(_)))
    }
    def apply[X](f: PartialFunction[X, List[X]]): Tweak[X] = {
      new Tweak(f.orElse { case _ => List.empty })
    }
  }

  // --[types]--

  private type Val = V
  private type KWM = GlobalKeyWithMaintainers
  private type OKWM = Option[KWM]
  private type Exe = Node.Exercise
  private type VTX = VersionedTransaction

  // --[samples]--

  private val samBool1 = true
  private val samBool2 = false

  private val somePkgName = PackageName.assertFromString("-default-package-name-")
  private val somePkgVer = Some(PackageVersion.assertFromString("1.0.0"))

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
  private val samValue2: Val = V.ValueText(samContractId1.coid)

  private val samKWM1 =
    GlobalKeyWithMaintainers.assertBuild(samTemplateId1, samValue1, samParties1, somePkgName)
  private val samKWM2 =
    GlobalKeyWithMaintainers.assertBuild(samTemplateId1, samValue1, samParties2, somePkgName)
  private val samKWM3 =
    GlobalKeyWithMaintainers.assertBuild(samTemplateId2, samValue2, samParties1, somePkgName)

  private val samVersion1: TransactionVersion = TransactionVersion.minVersion
  private val samVersion2: TransactionVersion = TransactionVersion.maxVersion

  private val someCreates: Seq[Node] =
    for {
      version <- Seq(samVersion1, samVersion2)
      key <- Seq(None, Some(samKWM1))
    } yield Node.Create(
      coid = samContractId1,
      packageName = somePkgName,
      packageVersion = somePkgVer.filter(_ => version >= TransactionVersion.minPackageVersion),
      templateId = samTemplateId1,
      arg = samValue1,
      signatories = samParties1,
      stakeholders = samParties2,
      keyOpt = key,
      version = version,
    )

  private val someFetches: Seq[Node] =
    for {
      version <- Seq(samVersion1, samVersion2)
      key <- Seq(None, Some(samKWM2))
      actingParties <- Seq(Set[Party](), Set(samParty1))
    } yield Node.Fetch(
      coid = samContractId1,
      packageName = somePkgName,
      templateId = samTemplateId1,
      actingParties = actingParties,
      signatories = samParties2,
      stakeholders = samParties3,
      keyOpt = key,
      byKey = samBool1,
      version = version,
    )

  private val someLookups: Seq[Node] =
    for {
      version <- Seq(samVersion1, samVersion2)
      result <- Seq(None, Some(samContractId1))
    } yield Node.LookupByKey(
      templateId = samTemplateId1,
      packageName = somePkgName,
      result = result,
      key = samKWM3,
      version = version,
    )

  private val someExercises: Seq[Exe] =
    for {
      version <- Seq(samVersion1, samVersion2)
      key <- Seq(None, Some(samKWM1))
      exerciseResult <- Seq(None, Some(samValue2))
    } yield Node.Exercise(
      targetCoid = samContractId2,
      packageName = somePkgName,
      templateId = samTemplateId2,
      // TODO https://github.com/digital-asset/daml/issues/13653
      //   also vary interfaceId (but this requires an interface choice)
      interfaceId = None,
      choiceId = samChoiceName1,
      consuming = samBool1,
      actingParties = samParties1,
      chosenValue = samValue1,
      stakeholders = samParties2,
      signatories = samParties3,
      choiceObservers = samParties4,
      choiceAuthorizers = None,
      children = ImmArray.Empty,
      exerciseResult = exerciseResult,
      keyOpt = key,
      byKey = samBool2,
      version = version,
    )

  // --[running tweaks]--
  // We dont aim for much coverage in the overal TX shape; we limit to either 0 or 1 level of nesting.

  private def flatVTXs(txVersion: TransactionVersion): Seq[VTX] =
    (someCreates ++ someFetches ++ someLookups ++ someExercises).map { node =>
      val nid = NodeId(0)
      VersionedTransaction(txVersion, HashMap(nid -> node), ImmArray(nid))
    }

  private def nestedVTXs(txVersion: TransactionVersion): Seq[VTX] =
    for {
      exe <- someExercises
      child <- someExercises ++ someCreates ++ someLookups ++ someFetches
    } yield {
      val nid0 = NodeId(0)
      val nid1 = NodeId(1)
      val parent = exe.copy(children = ImmArray(nid1))
      VersionedTransaction(
        txVersion,
        HashMap(nid0 -> parent, nid1 -> child),
        ImmArray(nid0),
      )
    }

  private def preTweakedVTXs: Seq[VTX] = {
    // we ensure the preTweaked txs are properly normalized.
    for {
      txVersion <- TransactionVersion.All
      vtx <- flatVTXs(txVersion) ++ nestedVTXs(txVersion)
    } yield Normalization.normalizeTx(vtx)
  }

  private def runTweak(tweak: Tweak[VTX]): Seq[(VTX, VTX)] =
    for {
      txA <- preTweakedVTXs
      txB <- tweak.run(txA)
    } yield (txA, txB)

  // --[changes]--
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

  // --[predicates]--
  // Some tweaks have version dependant significance.

  private def versionSinceMinByKey(v: TransactionVersion): Boolean = {
    import scala.Ordering.Implicits.infixOrderingOps
    v >= TransactionVersion.minContractKeys
  }

  // --[shared sub tweaks]--

  private val tweakPartySet = Tweak[Set[Party]] { case xs =>
    (xs + samPartyX) ::
      List(samParties1, samParties2, samParties3, samParties4).filter(set => set != xs)
  }

  private val tweakKeyMaintainers = Tweak[KWM] { case x =>
    List(samKWM1, samKWM2, samKWM3).filter(y => x != y)
  }

  private val tweakOptKeyMaintainers = Tweak[OKWM] {
    case None => List(Some(samKWM1), Some(samKWM2), Some(samKWM3))
    case Some(x) =>
      None :: List(samKWM1, samKWM2, samKWM3)
        .filter(y => x != y)
        .map(Some(_))
  }

  private val tweakOptContractId = Tweak[Option[V.ContractId]] { case x =>
    List(None, Some(samContractId1), Some(samContractId2)).filter(y => x != y)
  }

  // --[Create node tweaks]--

  private val tweakCreateCoid = Tweak.single[Node] { case nc: Node.Create =>
    nc.copy(coid = changeContractId(nc.coid))
  }
  private val tweakCreateTemplateId = Tweak.single[Node] { case nc: Node.Create =>
    nc.copy(templateId = changeTemplateId(nc.templateId))
  }
  private val tweakCreateArg = Tweak.single[Node] { case nc: Node.Create =>
    nc.copy(arg = changeValue(nc.arg))
  }
  private val tweakCreateSignatories = Tweak[Node] { case nc: Node.Create =>
    tweakPartySet.run(nc.signatories).map { x => nc.copy(signatories = x) }
  }
  private val tweakCreateStakeholders = Tweak[Node] { case nc: Node.Create =>
    tweakPartySet.run(nc.stakeholders).map { x => nc.copy(stakeholders = x) }
  }
  private def tweakCreateKey(tweakOptKeyMaintainers: Tweak[OKWM]) =
    Tweak[Node] { case nc: Node.Create =>
      tweakOptKeyMaintainers.run(nc.keyOpt).map { x => nc.copy(keyOpt = x) }
    }
  private val tweakCreateVersion = Tweak.single[Node] { case nc: Node.Create =>
    val version = changeVersion(nc.version)
    val pkgVer = nc.packageVersion
      .orElse(somePkgVer)
      .filter(_ => version >= TransactionVersion.minPackageVersion)
    nc.copy(version = version, packageVersion = pkgVer)
  }

  private val sigCreateTweaks =
    Map(
      "tweakCreateCoid" -> tweakCreateCoid,
      "tweakCreateTemplateId" -> tweakCreateTemplateId,
      "tweakCreateArg" -> tweakCreateArg,
      "tweakCreateSignatories" -> tweakCreateSignatories,
      "tweakCreateStakeholders" -> tweakCreateStakeholders,
      "tweakCreateKey" -> tweakCreateKey(tweakOptKeyMaintainers),
      "tweakCreateVersion" -> tweakCreateVersion,
    )

  // --[Fetch node tweaks]--

  private val tweakFetchCoid = Tweak.single[Node] { case nf: Node.Fetch =>
    nf.copy(coid = changeContractId(nf.coid))
  }
  private val tweakFetchTemplateId = Tweak.single[Node] { case nf: Node.Fetch =>
    nf.copy(templateId = changeTemplateId(nf.templateId))
  }
  private val tweakFetchActingPartiesNonEmpty = Tweak[Node] { case nf: Node.Fetch =>
    tweakPartySet.run(nf.actingParties).map { x => nf.copy(actingParties = x) }
  }
  private val tweakFetchSignatories = Tweak[Node] { case nf: Node.Fetch =>
    tweakPartySet.run(nf.signatories).map { x => nf.copy(signatories = x) }
  }
  private val tweakFetchStakeholders = Tweak[Node] { case nf: Node.Fetch =>
    tweakPartySet.run(nf.stakeholders).map { x => nf.copy(stakeholders = x) }
  }
  private def tweakFetchKey(tweakOptKeyMaintainers: TransactionVersion => Tweak[OKWM]) =
    Tweak[Node] { case nf: Node.Fetch =>
      tweakOptKeyMaintainers(nf.version).run(nf.keyOpt).map { x => nf.copy(keyOpt = x) }
    }
  private def tweakFetchByKey(whenVersion: TransactionVersion => Boolean) = Tweak.single[Node] {
    case nf: Node.Fetch if whenVersion(nf.version) =>
      nf.copy(byKey = changeBoolean(nf.byKey))
  }
  private val tweakFetchVersion = Tweak.single[Node] { case nf: Node.Fetch =>
    nf.copy(version = changeVersion(nf.version))
  }

  private val sigFetchTweaks =
    Map(
      "tweakFetchCoid" -> tweakFetchCoid,
      "tweakFetchTemplateId" -> tweakFetchTemplateId,
      "tweakFetchActingParties" -> tweakFetchActingPartiesNonEmpty,
      "tweakFetchSignatories" -> tweakFetchSignatories,
      "tweakFetchStakeholders" -> tweakFetchStakeholders,
      "tweakFetchKey" -> tweakFetchKey((_: TransactionVersion) => tweakOptKeyMaintainers),
      "tweakFetchByKey(New Version)" -> tweakFetchByKey(versionSinceMinByKey),
      "tweakFetchVersion" -> tweakFetchVersion,
    )

  // --[LookupByKey node tweaks]--

  private val tweakLookupTemplateId = Tweak.single[Node] { case nl: Node.LookupByKey =>
    nl.copy(templateId = changeTemplateId(nl.templateId))
  }
  private val tweakLookupKey = Tweak[Node] { case nl: Node.LookupByKey =>
    tweakKeyMaintainers.run(nl.key).map { x => nl.copy(key = x) }
  }
  private val tweakLookupResult = Tweak[Node] { case nl: Node.LookupByKey =>
    tweakOptContractId.run(nl.result).map { x => nl.copy(result = x) }
  }
  private val tweakLookupVersion = Tweak.single[Node] { case nl: Node.LookupByKey =>
    nl.copy(version = changeVersion(nl.version))
  }

  private val sigLookupTweaks =
    Map(
      "tweakLookupTemplateId" -> tweakLookupTemplateId,
      "tweakLookupKey" -> tweakLookupKey,
      "tweakLookupResult" -> tweakLookupResult,
      "tweakLookupVersion" -> tweakLookupVersion,
    )

  // --[Exercise node tweaks]--

  private val tweakExerciseTargetCoid = Tweak.single[Node] { case ne: Node.Exercise =>
    ne.copy(targetCoid = changeContractId(ne.targetCoid))
  }
  private val tweakExerciseTemplateId = Tweak.single[Node] { case ne: Node.Exercise =>
    ne.copy(templateId = changeTemplateId(ne.templateId))
  }
  private val tweakExerciseChoiceId = Tweak.single[Node] { case ne: Node.Exercise =>
    ne.copy(choiceId = changeChoiceId(ne.choiceId))
  }
  private val tweakExerciseConsuming = Tweak.single[Node] { case ne: Node.Exercise =>
    ne.copy(consuming = changeBoolean(ne.consuming))
  }
  private val tweakExerciseActingParties = Tweak[Node] { case ne: Node.Exercise =>
    tweakPartySet.run(ne.actingParties).map { x => ne.copy(actingParties = x) }
  }
  private val tweakExerciseChosenValue = Tweak.single[Node] { case ne: Node.Exercise =>
    ne.copy(chosenValue = changeValue(ne.chosenValue))
  }
  private val tweakExerciseStakeholders = Tweak[Node] { case ne: Node.Exercise =>
    tweakPartySet.run(ne.stakeholders).map { x => ne.copy(stakeholders = x) }
  }
  private val tweakExerciseSignatories = Tweak[Node] { case ne: Node.Exercise =>
    tweakPartySet.run(ne.signatories).map { x => ne.copy(signatories = x) }
  }
  private val tweakExerciseChoiceObservers = Tweak[Node] { case ne: Node.Exercise =>
    tweakPartySet.run(ne.choiceObservers).map { x => ne.copy(choiceObservers = x) }
  }
  private val tweakExerciseExerciseResult = Tweak[Node] { case ne: Node.Exercise =>
    ne.exerciseResult match {
      case None => List(ne.copy(exerciseResult = Some(samValue1)))
      case Some(v) =>
        List(
          ne.copy(exerciseResult = Some(changeValue(v))),
          ne.copy(exerciseResult = None),
        )
    }
  }

  private def tweakExerciseKey(tweakOptKeyMaintainers: TransactionVersion => Tweak[OKWM]) =
    Tweak[Node] { case ne: Node.Exercise =>
      tweakOptKeyMaintainers(ne.version).run(ne.keyOpt).map { x => ne.copy(keyOpt = x) }
    }
  private def tweakExerciseByKey(whenVersion: TransactionVersion => Boolean) = Tweak.single[Node] {
    case ne: Node.Exercise if whenVersion(ne.version) =>
      ne.copy(byKey = changeBoolean(ne.byKey))
  }
  private val tweakExerciseVersion = Tweak.single[Node] { case ne: Node.Exercise =>
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
      "tweakExerciseKey" -> tweakExerciseKey((_: TransactionVersion) => tweakOptKeyMaintainers),
      "tweakExerciseByKey(New Version)" -> tweakExerciseByKey(versionSinceMinByKey),
      "tweakExerciseVersion" -> tweakExerciseVersion,
    )

  // --[significant tx tweaks]--

  private def tweakTxNodes(tweakNode: Tweak[Node]) = Tweak[VTX] { vtx =>
    // tweak any node in a transaction
    vtx.transaction match {
      case Transaction(nodeMapA, roots) =>
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

  // --[per tweak tests]--

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
