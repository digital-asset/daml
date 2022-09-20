// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.data.Ref.Party
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SError.{SError, SErrorCrash}
import com.daml.lf.speedy.SExpr.SExpr
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.speedy.Speedy.{CachedContract, OnLedger}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.lf.value.Value.ContractId.`Cid Order`
import com.daml.lf.value.Value.ContractId.V1.`V1 Order`
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompilerTest extends AnyWordSpec with Matchers with Inside {

  import CompilerTest._

  "unsafeCompile" should {
    "handle 10k commands" in {
      val cmds = ImmArray.ImmArraySeq
        .fill(10 * 1000)(Command.Create(recordCon, contract()))
        .toImmArray

      compiledPackages.compiler.unsafeCompile(cmds) shouldBe a[SExpr]
    }

    "compile deeply nested lets" in {
      val expr = List
        .range[Long](1, 3000)
        .foldRight[Expr](EPrimLit(PLInt64(5000)))((i, acc) =>
          ELet(
            Binding(
              Some(Ref.Name.assertFromString(s"v$i")),
              TBuiltin(BTInt64),
              EPrimLit(PLInt64(i)),
            ),
            acc,
          )
        )

      compiledPackages.compiler.unsafeCompile(expr) shouldBe a[SExpr]
    }
  }

  "compileWithContractDisclosures" should {
    val version = TransactionVersion.minExplicitDisclosure
    val contractId1 = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id-1"))
    val contractId2 = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id-2"))

    "non-existent templates should crash compilation" in {
      val invalidTemplateId = Ref.Identifier.assertFromString("-pkgId-:Module:Invalid")
      val invalidDisclosedContract = buildDisclosedContract(contractId1, alice, invalidTemplateId)
      val invalidVersionedContract = VersionedContractInstance(
        version,
        invalidTemplateId,
        invalidDisclosedContract.argument.toUnnormalizedValue,
        "Agreement",
      )
      val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
        tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
        ImmArray(invalidDisclosedContract),
      )

      inside(evalSExpr(sexpr, getContract = Map(contractId1 -> invalidVersionedContract))) {
        case Left(SErrorCrash(_, message)) =>
          message should endWith(s"Template $invalidTemplateId does not exist and it should")
      }
    }

    "using a template with no key" should {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Module:Record")
      val disclosedContract1 = buildDisclosedContract(contractId1, alice, templateId)
      val versionedContract1 = VersionedContractInstance(
        version,
        templateId,
        disclosedContract1.argument.toUnnormalizedValue,
        "Agreement",
      )
      val disclosedContract2 = buildDisclosedContract(contractId2, alice, templateId)
      val versionedContract2 = VersionedContractInstance(
        version,
        templateId,
        disclosedContract2.argument.toUnnormalizedValue,
        "Agreement",
      )

      "with no commands" should {
        "contract cache empty with no disclosures" in {
          val sexpr =
            compiledPackages.compiler.unsafeCompileWithContractDisclosures(
              tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
              ImmArray.Empty,
            )

          inside(evalSExpr(sexpr)) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache shouldBe Map.empty
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains single disclosure" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
            ImmArray(disclosedContract1),
          )

          inside(evalSExpr(sexpr, getContract = Map(contractId1 -> versionedContract1))) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache.keySet shouldBe Set(contractId1)
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains multiple disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
            ImmArray(disclosedContract1, disclosedContract2),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract =
                Map(contractId1 -> versionedContract1, contractId2 -> versionedContract2),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet shouldBe Set(contractId1, contractId2)
            disclosedContractKeys shouldBe Map.empty
          }
        }
      }

      "with one command" should {
        val command = Command.Create(templateId, contract())

        "contract cache contains created contract with no disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command))),
            ImmArray.Empty,
          )

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache.keySet.size shouldBe 1
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains created contract and single disclosure" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command))),
            ImmArray(disclosedContract1),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(contractId1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 2
            contractCache.keySet should contain(contractId1)
            disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains created contract and multiple disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command))),
            ImmArray(disclosedContract1, disclosedContract2),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract =
                Map(contractId1 -> versionedContract1, contractId2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 3
            contractCache.keySet should contain(contractId1)
            contractCache.keySet should contain(contractId2)
            disclosedContractKeys shouldBe Map.empty
          }
        }
      }

      "with multiple commands" should {
        val command1 = Command.Create(templateId, contract())
        val command2 = Command.Create(templateId, contract())

        "contract cache contains all created contracts with no disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2))),
            ImmArray.Empty,
          )

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache.keySet.size shouldBe 2
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains all created contracts and single disclosure" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2))),
            ImmArray(disclosedContract1),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(contractId1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 3
            contractCache.keySet should contain(contractId1)
            disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains all created contracts and multiple disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2))),
            ImmArray(disclosedContract1, disclosedContract2),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract =
                Map(contractId1 -> versionedContract1, contractId2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 4
            contractCache.keySet should contain(contractId1)
            contractCache.keySet should contain(contractId2)
            disclosedContractKeys shouldBe Map.empty
          }
        }
      }
    }

    "using a template with a key" should {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Module:RecordKey")
      val disclosedContract1 =
        buildDisclosedContract(contractId1, alice, templateId, keyLabel = "test-label-1")
      val versionedContract1 = VersionedContractInstance(
        version,
        templateId,
        disclosedContract1.argument.toUnnormalizedValue,
        "Agreement",
      )
      val disclosedContract2 =
        buildDisclosedContract(contractId2, alice, templateId, keyLabel = "test-label-2")
      val versionedContract2 = VersionedContractInstance(
        version,
        templateId,
        disclosedContract2.argument.toUnnormalizedValue,
        "Agreement",
      )

      "with no commands" should {
        "contract cache is empty with no disclosures" in {
          val sexpr =
            compiledPackages.compiler.unsafeCompileWithContractDisclosures(
              tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
              ImmArray.Empty,
            )

          inside(evalSExpr(sexpr)) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache shouldBe Map.empty
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains single disclosure" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
            ImmArray(disclosedContract1),
          )

          inside(evalSExpr(sexpr, getContract = Map(contractId1 -> versionedContract1))) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache.keySet shouldBe Set(contractId1)
              disclosedContractKeys.values.map(_.value).toList.sorted shouldBe List(contractId1)
          }
        }

        "contract cache contains multiple disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty)),
            ImmArray(disclosedContract1, disclosedContract2),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract =
                Map(contractId1 -> versionedContract1, contractId2 -> versionedContract2),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet shouldBe Set(contractId1, contractId2)
            disclosedContractKeys.values.map(_.value).toList.sorted shouldBe List(
              contractId1,
              contractId2,
            ).sorted
          }
        }
      }

      "with one command" should {
        val command = Command.Create(templateId, contract("test-label"))

        "contract cache contains created contract with no disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command))),
            ImmArray.Empty,
          )

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache.keySet.size shouldBe 1
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains created contract and single disclosure" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command))),
            ImmArray(disclosedContract1),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(contractId1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 2
            contractCache.keySet should contain(contractId1)
            disclosedContractKeys.values.map(_.value).toList.sorted shouldBe List(contractId1)
          }
        }

        "contract cache contains created contract and multiple disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command))),
            ImmArray(disclosedContract1, disclosedContract2),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract =
                Map(contractId1 -> versionedContract1, contractId2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 3
            contractCache.keySet should contain(contractId1)
            contractCache.keySet should contain(contractId2)
            disclosedContractKeys.values.map(_.value).toList.sorted shouldBe List(
              contractId1,
              contractId2,
            ).sorted
          }
        }
      }

      "with multiple commands" should {
        val command1 = Command.Create(templateId, contract("test-label-1"))
        val command2 = Command.Create(templateId, contract("test-label-2"))

        "contract cache contains all created contracts with no disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2))),
            ImmArray.Empty,
          )

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
              contractCache.keySet.size shouldBe 2
              disclosedContractKeys shouldBe Map.empty
          }
        }

        "contract cache contains all created contracts and single disclosure" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2))),
            ImmArray(disclosedContract1),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(contractId1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 3
            contractCache.keySet should contain(contractId1)
            disclosedContractKeys.values.map(_.value).toList.sorted shouldBe List(contractId1)
          }
        }

        "contract cache contains all created contracts and multiple disclosures" in {
          val sexpr = compiledPackages.compiler.unsafeCompileWithContractDisclosures(
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2))),
            ImmArray(disclosedContract1, disclosedContract2),
          )

          inside(
            evalSExpr(
              sexpr,
              getContract =
                Map(contractId1 -> versionedContract1, contractId2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContractKeys)) =>
            contractCache.keySet.size shouldBe 4
            contractCache.keySet should contain(contractId1)
            contractCache.keySet should contain(contractId2)
            disclosedContractKeys.values.map(_.value).toList.sorted shouldBe List(
              contractId1,
              contractId2,
            ).sorted
          }
        }
      }
    }
  }
}

object CompilerTest {

  import defaultParserParameters.{defaultPackageId => pkgId}
  import SpeedyTestLib.loggingContext

  implicit val contractIdOrder: Ordering[ContractId] = `Cid Order`.toScalaOrdering
  implicit val contractIdV1Order: Ordering[ContractId.V1] = `V1 Order`.toScalaOrdering

  val recordCon: Ref.Identifier =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Record"))
  val pkg =
    p"""
        module Module {

          record @serializable Record = { label: Text, party: Party };
          template (this : Record) =  {
            precondition True;
            signatories Cons @Party [Module:Record {party} this] (Nil @Party);
            observers Nil @Party;
            agreement "Agreement";
          };

          record @serializable Key = { label: Text, party: Party };
          record @serializable RecordKey = { label: Text, party: Party };
          template (this : RecordKey) =  {
            precondition True;
            signatories Cons @Party [Module:RecordKey {party} this] (Nil @Party);
            observers Nil @Party;
            agreement "Agreement";
            key @Module:Key
              (Module:Key { label = Module:RecordKey {label} this, party = Module:RecordKey {party} this })
              (\(key: Module:Key) -> (Cons @Party [Module:Key {party} key] (Nil @Party)));
          };
        }
    """
  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(Map(pkgId -> pkg))
  val alice: Party = Ref.Party.assertFromString("Alice")

  def contract(label: String = ""): SValue.SRecord = SValue.SRecord(
    recordCon,
    ImmArray(Ref.Name.assertFromString("label"), Ref.Name.assertFromString("party")),
    ArrayList(SValue.SText(label), SValue.SParty(alice)),
  )

  def tokenApp(sexpr: SExpr): SExpr =
    SExpr.SEApp(sexpr, Array(SExpr.SEValue.Token))

  def evalSExpr(
      sexpr: SExpr,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      committers: Set[Party] = Set.empty,
  ): Either[
    SError,
    (SValue, Map[ContractId, CachedContract], Map[crypto.Hash, SValue.SContractId]),
  ] = {
    val machine =
      Speedy.Machine(
        compiledPackages = compiledPackages,
        submissionTime = Time.Timestamp.MinValue,
        initialSeeding = InitialSeeding.TransactionSeed(crypto.Hash.hashPrivateKey("CompilerTest")),
        expr = sexpr,
        committers = committers,
        disclosedContracts = ImmArray.Empty,
        readAs = Set.empty,
      )

    SpeedyTestLib.run(machine, getContract = getContract).map { value =>
      machine.ledgerMode match {
        case onLedger: OnLedger =>
          (value, onLedger.cachedContracts, onLedger.disclosureKeyTable.toMap)

        case _ =>
          (value, Map.empty, Map.empty)
      }
    }
  }

  def buildDisclosedContract(
      contractId: ContractId,
      maintainer: Party,
      templateId: Ref.Identifier,
      keyLabel: String = "",
  ): DisclosedContract = {
    val withKey = keyLabel.nonEmpty
    val key = SValue.SRecord(
      templateId,
      ImmArray(
        Ref.Name.assertFromString("label"),
        Ref.Name.assertFromString("party"),
      ),
      ArrayList(
        SValue.SText(keyLabel),
        SValue.SList(FrontStack(SValue.SParty(maintainer))),
      ),
    )
    val globalKey =
      if (withKey) {
        Some(
          GlobalKeyWithMaintainers(
            GlobalKey(templateId, key.toUnnormalizedValue),
            Set(maintainer),
          )
        )
      } else {
        None
      }
    val keyHash = globalKey.map(_.globalKey.hash)
    val disclosedContract = DisclosedContract(
      templateId,
      SContractId(contractId),
      contract(keyLabel),
      ContractMetadata(Time.Timestamp.now(), keyHash, ImmArray.Empty),
    )

    disclosedContract
  }
}
