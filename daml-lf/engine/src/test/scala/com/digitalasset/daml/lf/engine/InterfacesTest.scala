// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.io.File
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.value.Value
import Value._
import com.daml.lf.command._
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class InterfacesTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import InterfacesTest._

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  private[this] val suffixLenientEngine = newEngine()
  private[this] val preprocessor =
    new preprocessing.Preprocessor(
      ConcurrentCompiledPackages(suffixLenientEngine.config.getCompilerConfig)
    )

  "interfaces" should {
    val (interfacesPkgId, _, allInterfacesPkgs) = loadPackage("daml-lf/tests/Interfaces.dar")
    val lookupPackage = allInterfacesPkgs.get(_)
    val idI1 = Identifier(interfacesPkgId, "Interfaces:I1")
    val idI2 = Identifier(interfacesPkgId, "Interfaces:I2")
    val idT1 = Identifier(interfacesPkgId, "Interfaces:T1")
    val idT2 = Identifier(interfacesPkgId, "Interfaces:T2")
    val let = Time.Timestamp.now()
    val submissionSeed = hash("rollback")
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val cid1 = toContractId("1")
    val cid2 = toContractId("2")
    val contracts = Map(
      cid1 -> assertAsVersionedContract(
        ContractInstance(
          idT1,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "",
        )
      ),
      cid2 -> assertAsVersionedContract(
        ContractInstance(
          idT2,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "",
        )
      ),
    )
    val lookupContract = contracts.get(_)
    def lookupKey = (_: GlobalKeyWithMaintainers) => None
    def preprocess(cmd: Command) = {
      preprocessor
        .preprocessCommand(cmd)
        .consume(
          lookupContract,
          lookupPackage,
          lookupKey,
        )
    }
    def run(cmd: Command) = {
      val submitters = Set(party)
      val Right(speedyCmd) = preprocess(cmd)
      suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = ImmArray(speedyCmd),
          ledgerTime = let,
          submissionTime = let,
          seeding = seeding,
        )
        .consume(
          lookupContract,
          lookupPackage,
          lookupKey,
        )
    }

    /* create by interface tests */
    "be able to create T1 by interface I1" in {
      val command =
        CreateByInterfaceCommand(idI1, idT1, ValueRecord(None, ImmArray((None, ValueParty(party)))))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to create T2 by interface I1" in {
      val command =
        CreateByInterfaceCommand(idI1, idT2, ValueRecord(None, ImmArray((None, ValueParty(party)))))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to create T2 by interface I2" in {
      val command =
        CreateByInterfaceCommand(idI2, idT2, ValueRecord(None, ImmArray((None, ValueParty(party)))))
      run(command) shouldBe a[Right[_, _]]
    }
    "be unable to create T1 by interface I2 (stopped in preprocessor)" in {
      val command =
        CreateByInterfaceCommand(idI2, idT1, ValueRecord(None, ImmArray((None, ValueParty(party)))))
      preprocess(command) shouldBe a[Left[_, _]]
    }

    /* generic exercise tests */
    "be able to exercise interface I1 on a T1 contract" in {
      val command = ExerciseCommand(idI1, cid1, "C1", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise interface I1 on a T2 contract" in {
      val command = ExerciseCommand(idI1, cid2, "C1", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise interface I2 on a T2 contract" in {
      val command = ExerciseCommand(idI2, cid2, "C2", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be unable to exercise interface I2 on a T1 contract" in {
      val command = ExerciseCommand(idI2, cid1, "C2", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Left[_, _]]
    }

    "be able to exercise T1 by interface I1" in {
      val command = ExerciseCommand(idT1, cid1, "C1", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise T2 by interface I1" in {
      val command = ExerciseCommand(idT2, cid2, "C1", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise T2 by interface I2" in {
      val command = ExerciseCommand(idT2, cid2, "C2", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be unable to exercise T1 by interface I2 (stopped in preprocessor)" in {
      val command = ExerciseCommand(idT1, cid1, "C2", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }

    // TODO https://github.com/digital-asset/daml/issues/11703
    //   Enable these tests.
    /*
      "be unable to exercise T1 (disguised as T2) by interface I1" in {
        val command = ExerciseCommand(idT2, cid1, "C1", ValueRecord(None, ImmArray.empty))
        run(command) shouldBe a[Left[_, _]]
      }
      "be unable to exercise T2 (disguised as T1) by interface I1" in {
        val command = ExerciseCommand(idT1, cid2, "C1", ValueRecord(None, ImmArray.empty))
        run(command) shouldBe a[Left[_, _]]
      }
    */
    "be unable to exercise T2 (disguised as T1) by interface I2 (stopped in preprocessor)" in {
      val command = ExerciseCommand(idT1, cid2, "C2", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }
    "be unable to exercise T1 (disguised as T2) by interface I2 " in {
      val command = ExerciseCommand(idT2, cid1, "C2", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Left[_, _]]
    }

    /* exercise template tests */
    "be unable to exercise interface via exercise template (stopped in preprocessor)" in {
      val command = ExerciseTemplateCommand(idI1, cid1, "C1", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }
    // TODO https://github.com/digital-asset/daml/issues/11558
    //   Fix lookupTemplateChoice to not return inherited choices.
    /*
      "be unable to exercise T1 inherited choice via exercise template (stopped in preprocessor)" in {
        val command = ExerciseTemplateCommand(idT1, cid1, "C1", ValueRecord(None, ImmArray.empty))
        preprocess(command) shouldBe a[Left[_, _]]
      }
     */
    "be able to exercise T1 own choice via exercise template" in {
      val command =
        ExerciseTemplateCommand(idT1, cid1, "OwnChoice", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }

    /* exercise by interface tests */
    "be unable to exercise interface via 'exercise by interface' (stopped in preprocessor)" in {
      val command =
        ExerciseByInterfaceCommand(idI1, idI1, cid1, "C1", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }

    "be able to exercise T1 by interface I1 via 'exercise by interface'" in {
      val command =
        ExerciseByInterfaceCommand(idI1, idT1, cid1, "C1", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise T2 by interface I1 via 'exercise by interface'" in {
      val command =
        ExerciseByInterfaceCommand(idI1, idT2, cid2, "C1", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise T2 by interface I2 via 'exercise by interface'" in {
      val command =
        ExerciseByInterfaceCommand(idI2, idT2, cid2, "C2", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Right[_, _]]
    }
    "be unable to exercise T1 by interface I2   (stopped in preprocessor)" in {
      val command =
        ExerciseByInterfaceCommand(idI2, idT1, cid1, "C2", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }

    // TODO https://github.com/digital-asset/daml/issues/11703
    //   Enable these tests.
    /*
      "be unable to exercise T1 (disguised as T2) by interface I1 via 'exercise by interface'" in {
        val command = ExerciseByInterfaceCommand(idI2, idT2, cid1, "C1", ValueRecord(None, ImmArray.empty))
        run(command) shouldBe a[Left[_, _]]
      }
      "be unable to exercise T2 (disguised as T1) by interface I1 via 'exercise by interface'" in {
        val command = ExerciseByInterfaceCommand(idI1, idT1, cid2, "C1", ValueRecord(None, ImmArray.empty))
        run(command) shouldBe a[Left[_, _]]
      }
     */
    "be unable to exercise T2 (disguised as T1) by interface I2 via 'exercise by interface' (stopped in preprocessor)" in {
      val command =
        ExerciseByInterfaceCommand(idI2, idT1, cid2, "C2", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }
    "be unable to exercise T1 (disguised as T2) by interface I2 via 'exercise by interface'" in {
      val command =
        ExerciseByInterfaceCommand(idI2, idT2, cid1, "C2", ValueRecord(None, ImmArray.empty))
      run(command) shouldBe a[Left[_, _]]
    }

    "be unable to exercise T2 choice by the wrong interface (stopped in preprocessor)" in {
      val command =
        ExerciseByInterfaceCommand(idI1, idT2, cid2, "C2", ValueRecord(None, ImmArray.empty))
      preprocess(command) shouldBe a[Left[_, _]]
    }

    /* fetch by interface tests */
    "be able to fetch T1 by interface I1" in {
      val command = FetchByInterfaceCommand(idI1, idT1, cid1)
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to fetch T2 by interface I1" in {
      val command = FetchByInterfaceCommand(idI1, idT2, cid2)
      run(command) shouldBe a[Right[_, _]]
    }
    "be able to fetch T2 by interface I2" in {
      val command = FetchByInterfaceCommand(idI2, idT2, cid2)
      run(command) shouldBe a[Right[_, _]]
    }
    "be unable to fetch T1 by interface I2 (stopped in preprocessor)" in {
      val command = FetchByInterfaceCommand(idI2, idT1, cid1)
      preprocess(command) shouldBe a[Left[_, _]]
    }
    "be unable to fetch T1 (disguised as T2) via interface I2" in {
      val command = FetchByInterfaceCommand(idI2, idT2, cid1)
      run(command) shouldBe a[Left[_, _]]
    }
    // TODO https://github.com/digital-asset/daml/issues/11703
    //   Enable these tests.
    /*
      "be unable to fetch T1 (disguised as T2) via interface I1" in {
        val command = FetchByInterfaceCommand(idI1, idT2, cid1)
        run(command) shouldBe a[Left[_, _]]
      }
      "be unable to fetch T2 (disguised as T1) via interface I1" in {
        val command = FetchByInterfaceCommand(idI1, idT1, cid2)
        run(command) shouldBe a[Left[_, _]]
      }
     */
    "be unable to fetch T2 (disguised as T1) by interface I2 (stopped in preprocessor)" in {
      val command = FetchByInterfaceCommand(idI2, idT1, cid2)
      preprocess(command) shouldBe a[Left[_, _]]
    }
  }

}

object InterfacesTest {

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")

  private val party = Party.assertFromString("Party")

  private def newEngine(requireCidSuffixes: Boolean = false) =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = language.LanguageVersion.DevVersions,
        forbidV0ContractId = true,
        requireSuffixedGlobalContractId = requireCidSuffixes,
      )
    )

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)
}
