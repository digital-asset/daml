// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.Party
import com.daml.lf.data._
import com.daml.lf.interpretation.Error.TemplatePreconditionViolated
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SError.{SError, SErrorDamlException}
import com.daml.lf.speedy.SExpr.SExpr
import com.daml.lf.speedy.Speedy.ContractInfo
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInstance}
import com.daml.lf.value.Value.ContractId.`Cid Order`
import com.daml.lf.value.Value.ContractId.V1.`V1 Order`
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompilerTestV2 extends CompilerTest(LanguageMajorVersion.V2)

class CompilerTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with Inside {

  val helpers = new CompilerTestHelpers(majorLanguageVersion)
  import helpers._

  "unsafeCompile" should {
    "handle 10k commands" in {
      val cmds = ImmArray.ImmArraySeq
        .fill(10 * 1000)(Command.Create(recordCon, contract()))
        .toImmArray

      compiledPackages.compiler.unsafeCompile(cmds, ImmArray.Empty) shouldBe a[SExpr]
    }

    "compile deeply nested lets" in {
      val expr = List
        .range[Long](1, 3000)
        .foldRight[Expr](EBuiltinLit(BLInt64(5000)))((i, acc) =>
          ELet(
            Binding(
              Some(Ref.Name.assertFromString(s"v$i")),
              TBuiltin(BTInt64),
              EBuiltinLit(BLInt64(i)),
            ),
            acc,
          )
        )

      compiledPackages.compiler.unsafeCompile(expr) shouldBe a[SExpr]
    }
  }

  "compileWithContractDisclosures" should {
    val version = TransactionVersion.assignNodeVersion(pkg.languageVersion)
    val cid1 = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id-1"))
    val cid2 = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id-2"))
    val disclosedCid1 =
      Value.ContractId.V1(crypto.Hash.hashPrivateKey("disclosed-test-contract-id-1"))
    val disclosedCid2 =
      Value.ContractId.V1(crypto.Hash.hashPrivateKey("disclosed-test-contract-id-2"))

    "using a template with preconditions" should {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Module:PreCondRecord")
      val disclosedContract1 =
        buildDisclosedContractWithPreCond(disclosedCid1, templateId, precondition = true)
      val versionedContract1 = Versioned(
        version = version,
        ContractInstance(
          packageName = pkg.name,
          template = templateId,
          arg = disclosedContract1.argument.toUnnormalizedValue,
        ),
      )
      val disclosedContract2 =
        buildDisclosedContractWithPreCond(cid2, templateId, precondition = false)
      val versionedContract2 = Versioned(
        version = version,
        ContractInstance(
          packageName = pkg.name,
          template = templateId,
          arg = disclosedContract2.argument.toUnnormalizedValue,
        ),
      )

      "accept disclosed contracts with a valid precondition" in {
        val sexpr = tokenApp(
          compiledPackages.compiler.unsafeCompile(ImmArray.Empty, ImmArray(disclosedContract1))
        )

        inside(evalSExpr(sexpr, getContract = Map(cid1 -> versionedContract1))) {
          case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache shouldBe empty
            disclosedContracts.keySet shouldBe Set(disclosedCid1)
        }
      }

      "reject disclosed contracts with an invalid precondition" in {
        val sexpr = tokenApp(
          compiledPackages.compiler.unsafeCompile(ImmArray.Empty, ImmArray(disclosedContract2))
        )

        inside(evalSExpr(sexpr, getContract = Map(cid2 -> versionedContract2))) {
          case Left(
                SErrorDamlException(TemplatePreconditionViolated(`templateId`, None, contract))
              ) =>
            contract shouldBe versionedContract2.unversioned.arg
        }
      }
    }

    "using a template with no key" should {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Module:Record")
      val disclosedContract1 = buildDisclosedContract(disclosedCid1, alice, templateId)
      val versionedContract1 = Versioned(
        version = version,
        ContractInstance(
          packageName = pkg.name,
          template = templateId,
          arg = disclosedContract1.argument.toUnnormalizedValue,
        ),
      )
      val disclosedContract2 = buildDisclosedContract(disclosedCid2, alice, templateId)
      val versionedContract2 = Versioned(
        version = version,
        ContractInstance(
          packageName = pkg.name,
          template = templateId,
          arg = disclosedContract2.argument.toUnnormalizedValue,
        ),
      )

      "with no commands" should {
        "contract cache empty with no disclosures" in {
          val sexpr =
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty, ImmArray.Empty))

          inside(evalSExpr(sexpr)) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet shouldBe empty
              disclosedContracts.keySet shouldBe empty
          }
        }

        "contract cache contains single disclosure" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(ImmArray.Empty, ImmArray(disclosedContract1))
          )

          inside(evalSExpr(sexpr, getContract = Map(cid1 -> versionedContract1))) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet shouldBe empty
              disclosedContracts.keySet shouldBe Set(disclosedCid1)
          }
        }

        "contract cache contains multiple disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler
              .unsafeCompile(ImmArray.Empty, ImmArray(disclosedContract1, disclosedContract2))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1, cid2 -> versionedContract2),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet shouldBe empty
            disclosedContracts.keySet shouldBe Set(disclosedCid1, disclosedCid2)
          }
        }
      }

      "with one command" should {
        val command = Command.Create(templateId, contract())

        "contract cache contains created contract with no disclosures" in {
          val sexpr =
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command), ImmArray.Empty))

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet.size shouldBe 1
              disclosedContracts.keySet shouldBe empty
          }
        }

        "contract cache contains created contract and single disclosure" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(ImmArray(command), ImmArray(disclosedContract1))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 1
            contractCache.keySet shouldNot contain(disclosedCid1)
            disclosedContracts.keySet shouldBe Set(disclosedCid1)
          }
        }

        "contract cache contains created contract and multiple disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler
              .unsafeCompile(ImmArray(command), ImmArray(disclosedContract1, disclosedContract2))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1, cid2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 1
            contractCache.keySet shouldNot contain(disclosedCid1)
            contractCache.keySet shouldNot contain(disclosedCid2)
            disclosedContracts.keySet shouldBe Set(disclosedCid1, disclosedCid2)
          }
        }
      }

      "with multiple commands" should {
        val command1 = Command.Create(templateId, contract())
        val command2 = Command.Create(templateId, contract())

        "contract cache contains all created contracts with no disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2), ImmArray.Empty)
          )

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet.size shouldBe 2
              disclosedContracts.keySet shouldBe empty
          }
        }

        "contract cache contains all created contracts and single disclosure" in {
          val sexpr = tokenApp(
            compiledPackages.compiler
              .unsafeCompile(ImmArray(command1, command2), ImmArray(disclosedContract1))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 2
            contractCache.keySet shouldNot contain(disclosedCid1)
            contractCache.keySet shouldNot contain(disclosedCid2)
            disclosedContracts.keySet shouldBe Set(disclosedCid1)
          }
        }

        "contract cache contains all created contracts and multiple disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(
              ImmArray(command1, command2),
              ImmArray(disclosedContract1, disclosedContract2),
            )
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1, cid2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 2
            contractCache.keySet shouldNot contain(disclosedCid1)
            contractCache.keySet shouldNot contain(disclosedCid2)
            disclosedContracts.keySet shouldBe Set(disclosedCid1, disclosedCid2)
          }
        }
      }
    }

    "using a template with a key" should {
      val templateId = Ref.Identifier.assertFromString("-pkgId-:Module:RecordKey")
      val disclosedContract1 =
        buildDisclosedContract(disclosedCid1, alice, templateId, keyLabel = "test-label-1")
      val versionedContract1 = Versioned(
        version = version,
        ContractInstance(
          packageName = pkg.name,
          template = templateId,
          arg = disclosedContract1.argument.toUnnormalizedValue,
        ),
      )
      val disclosedContract2 =
        buildDisclosedContract(disclosedCid2, alice, templateId, keyLabel = "test-label-2")
      val versionedContract2 = Versioned(
        version = version,
        ContractInstance(
          packageName = pkg.name,
          template = templateId,
          arg = disclosedContract2.argument.toUnnormalizedValue,
        ),
      )

      "with no commands" should {
        "contract cache is empty with no disclosures" in {
          val sexpr =
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray.Empty, ImmArray.Empty))

          inside(evalSExpr(sexpr)) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet shouldBe empty
              disclosedContracts.keySet shouldBe empty
          }
        }

        "contract cache contains single disclosure" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(ImmArray.Empty, ImmArray(disclosedContract1))
          )

          inside(evalSExpr(sexpr, getContract = Map(cid1 -> versionedContract1))) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet shouldBe empty
              disclosedContracts.keySet shouldBe Set(disclosedCid1)
          }
        }

        "contract cache contains multiple disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler
              .unsafeCompile(ImmArray.Empty, ImmArray(disclosedContract1, disclosedContract2))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1, cid2 -> versionedContract2),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet shouldBe empty
            disclosedContracts.keySet shouldBe Set(disclosedCid1, disclosedCid2)
          }
        }
      }

      "with one command" should {
        val command = Command.Create(templateId, contract("test-label"))

        "contract cache contains created contract with no disclosures" in {
          val sexpr =
            tokenApp(compiledPackages.compiler.unsafeCompile(ImmArray(command), ImmArray.Empty))

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet.size shouldBe 1
              disclosedContracts.keySet shouldBe empty
          }
        }

        "contract cache contains created contract and single disclosure" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(ImmArray(command), ImmArray(disclosedContract1))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 1
            contractCache.keySet shouldNot contain(disclosedCid1)
            disclosedContracts.keySet shouldBe Set(disclosedCid1)
          }
        }

        "contract cache contains created contract and multiple disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler
              .unsafeCompile(ImmArray(command), ImmArray(disclosedContract1, disclosedContract2))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1, cid2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 1
            contractCache.keySet shouldNot contain(disclosedCid1)
            contractCache.keySet shouldNot contain(disclosedCid2)
            disclosedContracts.keySet shouldBe Set(disclosedCid1, disclosedCid2)
          }
        }
      }

      "with multiple commands" should {
        val command1 = Command.Create(templateId, contract("test-label-1"))
        val command2 = Command.Create(templateId, contract("test-label-2"))

        "contract cache contains all created contracts with no disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(ImmArray(command1, command2), ImmArray.Empty)
          )

          inside(evalSExpr(sexpr, committers = Set(alice))) {
            case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
              contractCache.keySet.size shouldBe 2
              disclosedContracts.keySet shouldBe empty
          }
        }

        "contract cache contains all created contracts and single disclosure" in {
          val sexpr = tokenApp(
            compiledPackages.compiler
              .unsafeCompile(ImmArray(command1, command2), ImmArray(disclosedContract1))
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 2
            contractCache.keySet shouldNot contain(disclosedCid1)
            disclosedContracts.keySet shouldBe Set(disclosedCid1)
          }
        }

        "contract cache contains all created contracts and multiple disclosures" in {
          val sexpr = tokenApp(
            compiledPackages.compiler.unsafeCompile(
              ImmArray(command1, command2),
              ImmArray(disclosedContract1, disclosedContract2),
            )
          )

          inside(
            evalSExpr(
              sexpr,
              getContract = Map(cid1 -> versionedContract1, cid2 -> versionedContract2),
              committers = Set(alice),
            )
          ) { case Right((SValue.SUnit, contractCache, disclosedContracts)) =>
            contractCache.keySet.size shouldBe 2
            contractCache.keySet shouldNot contain(disclosedCid1)
            contractCache.keySet shouldNot contain(disclosedCid2)
            disclosedContracts.keySet shouldBe Set(disclosedCid1, disclosedCid2)
          }
        }
      }
    }
  }
}

final class CompilerTestHelpers(majorLanguageVersion: LanguageMajorVersion) {

  import SpeedyTestLib.loggingContext

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)
  val pkgId = parserParameters.defaultPackageId

  implicit val contractIdOrder: Ordering[ContractId] = `Cid Order`.toScalaOrdering
  implicit val contractIdV1Order: Ordering[ContractId.V1] = `V1 Order`.toScalaOrdering

  val recordCon: Ref.Identifier =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Record"))
  val preCondRecordCon: Ref.Identifier =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:PreCondRecord"))
  val pkg =
    p"""  metadata ( '-compiler-test-package-' : '1.0.0' )
        module Module {

          record @serializable Record = { label: Text, party: Party };
          template (this : Record) =  {
            precondition True;
            signatories Cons @Party [Module:Record {party} this] (Nil @Party);
            observers Nil @Party;
          };

          record @serializable PreCondRecord = { precond: Bool, party: Party };
          template (this : PreCondRecord) =  {
            precondition Module:PreCondRecord {precond} this;
            signatories Cons @Party [Module:PreCondRecord {party} this] (Nil @Party);
            observers Nil @Party;
          };

          record @serializable Key = { label: Text, party: Party };
          record @serializable RecordKey = { label: Text, party: Party };
          template (this : RecordKey) =  {
            precondition True;
            signatories Cons @Party [Module:RecordKey {party} this] (Nil @Party);
            observers Nil @Party;
            key @Module:Key
              (Module:Key { label = Module:RecordKey {label} this, party = Module:RecordKey {party} this })
              (\(key: Module:Key) -> (Cons @Party [Module:Key {party} key] (Nil @Party)));
          };
        }
    """
  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(
      Map(pkgId -> pkg),
      Compiler.Config.Default(majorLanguageVersion),
    )
  val alice: Party = Ref.Party.assertFromString("Alice")

  def contract(label: String = ""): SValue.SRecord = SValue.SRecord(
    recordCon,
    ImmArray(Ref.Name.assertFromString("label"), Ref.Name.assertFromString("party")),
    ArrayList(SValue.SText(label), SValue.SParty(alice)),
  )

  def preCondContract(precondition: Boolean): SValue.SRecord = SValue.SRecord(
    preCondRecordCon,
    ImmArray(Ref.Name.assertFromString("precond"), Ref.Name.assertFromString("party")),
    ArrayList(SValue.SBool(precondition), SValue.SParty(alice)),
  )

  def tokenApp(sexpr: SExpr): SExpr =
    SExpr.SEApp(sexpr, Array(SValue.SToken))

  def evalSExpr(
      sexpr: SExpr,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      committers: Set[Party] = Set.empty,
  ): Either[
    SError,
    (SValue, Map[ContractId, (Ref.Identifier, SValue)], Map[ContractId, ContractInfo]),
  ] = {
    val machine =
      Speedy.UpdateMachine(
        compiledPackages = compiledPackages,
        submissionTime = Time.Timestamp.MinValue,
        initialSeeding = InitialSeeding.TransactionSeed(crypto.Hash.hashPrivateKey("CompilerTest")),
        expr = sexpr,
        committers = committers,
        readAs = Set.empty,
      )

    SpeedyTestLib
      .run(machine, getContract = getContract)
      .map((_, machine.localContractStore, machine.disclosedContracts))
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
        SValue.SParty(maintainer),
      ),
    )
    val globalKey =
      if (withKey) {
        Some(
          GlobalKeyWithMaintainers(
            GlobalKey.assertBuild(templateId, key.toUnnormalizedValue),
            Set(maintainer),
          )
        )
      } else {
        None
      }
    val keyHash = globalKey.map(_.globalKey.hash)
    val disclosedContract = DisclosedContract(
      templateId,
      contractId,
      key,
      keyHash,
    )

    disclosedContract
  }

  def buildDisclosedContractWithPreCond(
      contractId: ContractId,
      templateId: Ref.Identifier,
      precondition: Boolean,
  ): DisclosedContract = {
    DisclosedContract(
      templateId,
      contractId,
      preCondContract(precondition = precondition),
      None,
    )
  }
}
