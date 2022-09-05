// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.command.{ApiCommand, ApiCommands}
import com.daml.lf.data.{Bytes, FrontStack, ImmArray, TemplateOrInterface, Time}
import com.daml.lf.data.Ref.{Identifier, Name, PackageId, ParticipantId, Party, QualifiedName}
import com.daml.lf.language.Ast.Package
import com.daml.lf.ledger.FailedAuthorization.{
  CreateMissingAuthorization,
  ExerciseMissingAuthorization,
}
import com.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  SubmittedTransaction,
  TransactionVersion,
  Versioned,
}
import com.daml.lf.transaction.Transaction.Metadata
import com.daml.lf.value.Value.{
  ContractId,
  ContractInstance,
  ValueContractId,
  ValueList,
  ValueParty,
  ValueRecord,
  VersionedContractInstance,
}
import com.daml.logging.LoggingContext

import java.io.File
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class AuthPropagationSpec extends AnyFreeSpec with Matchers with Inside with BazelRunfiles {

  private[this] implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  implicit private def toName(s: String): Name = Name.assertFromString(s)
  implicit private def toParty(s: String): Party = Party.assertFromString(s)

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  private val (packageId, _, allPackages) = loadPackage(
    "daml-lf/tests/AuthTests.dar"
  )

  implicit private def toIdentifier(s: String): Identifier =
    Identifier(packageId, QualifiedName.assertFromString(s"AuthTests:$s"))

  private def toContractId(s: String): ContractId = {
    val dummySuffix: Bytes = Bytes.assertFromString("00")
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)
  }

  private def t1InstanceFor(party: Party): VersionedContractInstance =
    Versioned(
      TransactionVersion.VDev,
      ContractInstance(
        "T1",
        ValueRecord(
          Some("T1"),
          ImmArray((Some[Name]("party"), ValueParty(party))),
        ),
        "",
      ),
    )

  private def x1InstanceFor(party: Party): VersionedContractInstance =
    Versioned(
      TransactionVersion.VDev,
      ContractInstance(
        "X1",
        ValueRecord(
          Some("X1"),
          ImmArray((Some[Name]("party"), ValueParty(party))),
        ),
        "",
      ),
    )

  private val defaultContracts: Map[ContractId, VersionedContractInstance] =
    Map(
      toContractId("t1a") -> t1InstanceFor("Alice"),
      toContractId("t1b") -> t1InstanceFor("Bob"),
      toContractId("x1b") -> x1InstanceFor("Bob"),
      toContractId("x1c") -> x1InstanceFor("Charlie"),
    )

  private val readAs: Set[Party] = Set.empty
  private val let: Time.Timestamp = Time.Timestamp.now()
  private val participant: ParticipantId = ParticipantId.assertFromString("participant")
  private val submissionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("submissionSeed")

  private val lookupPackage: PackageId => Option[Package] =
    pkgId => allPackages.get(pkgId)

  private val lookupContract: ContractId => Option[VersionedContractInstance] =
    cid => defaultContracts.get(cid)

  private val lookupKey: GlobalKeyWithMaintainers => Option[ContractId] =
    _ => None

  private val testEngine: Engine =
    Engine.DevEngine()

  private def go(
      submitters: Set[Party],
      command: ApiCommand,
  ): Either[engine.Error, (SubmittedTransaction, Metadata)] = {

    val interpretResult =
      testEngine
        .submit(
          submitters,
          readAs,
          ApiCommands(ImmArray(command), let, "commands-tag"),
          ImmArray.empty,
          participant,
          submissionSeed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

    interpretResult
  }

  "Create(T1)" - {
    val command: ApiCommand =
      ApiCommand.Create(
        "T1",
        ValueRecord(
          Some("T1"),
          ImmArray(
            (Some[Name]("party"), ValueParty("Alice"))
          ),
        ),
      )
    "ok" in {
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
    "fail" in {
      val interpretResult = go(
        submitters = Set.empty,
        command = command,
      )
      inside(interpretResult) {
        case Left(
              engine.Error.Interpretation(
                engine.Error.Interpretation.DamlException(
                  interpretation.Error
                    .FailedAuthorization(_, CreateMissingAuthorization(_, _, _, _))
                ),
                _,
              )
            ) =>
      }
    }
  }

  "Create(T2)" - {
    val command: ApiCommand =
      ApiCommand.Create(
        "T2",
        ValueRecord(
          Some("T2"),
          ImmArray(
            (Some[Name]("party1"), ValueParty("Alice")),
            (Some[Name]("party2"), ValueParty("Bob")),
          ),
        ),
      )

    "ok" in {
      val interpretResult = go(
        submitters = Set("Alice", "Bob"),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
    "fail" in {
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      inside(interpretResult) {
        case Left(
              engine.Error.Interpretation(
                engine.Error.Interpretation.DamlException(
                  interpretation.Error
                    .FailedAuthorization(_, CreateMissingAuthorization(_, _, _, _))
                ),
                _,
              )
            ) =>
      }
    }
  }

  // Test authorization of an exercise choice collects authority from both
  // the contract signatory and the choice controller

  "Exercise(Choice1 of T1 to create T2)" - {

    // TEST_EVIDENCE: Authorization: well-authorized exercise/create is accepted
    "ok (Alice signed contract; Bob exercised Choice)" in {
      val command: ApiCommand =
        ApiCommand.Exercise(
          TemplateOrInterface.Template("T1"),
          toContractId("t1a"),
          "Choice1",
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty("Alice")),
              (Some[Name]("party2"), ValueParty("Bob")),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set("Bob"),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }

    // TEST_EVIDENCE: Authorization: badly-authorized exercise/create (exercise is unauthorized) is rejected
    "fail: ExerciseMissingAuthorization" in {
      val command: ApiCommand =
        ApiCommand.Exercise(
          TemplateOrInterface.Template("T1"),
          toContractId("t1a"),
          "Choice1",
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty("Alice")),
              (Some[Name]("party2"), ValueParty("Bob")),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      inside(interpretResult) {
        case Left(
              engine.Error.Interpretation(
                engine.Error.Interpretation.DamlException(
                  interpretation.Error
                    .FailedAuthorization(_, ExerciseMissingAuthorization(_, _, _, _, _))
                ),
                _,
              )
            ) =>
      }
    }

    // TEST_EVIDENCE: Authorization: badly-authorized exercise/create (create is unauthorized) is rejected
    "fail: CreateMissingAuthorization" in {
      val command: ApiCommand =
        ApiCommand.Exercise(
          TemplateOrInterface.Template("T1"),
          toContractId("t1a"),
          "Choice1",
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty("Bob")),
              (Some[Name]("party2"), ValueParty("Alice")),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      inside(interpretResult) {
        case Left(
              engine.Error.Interpretation(
                engine.Error.Interpretation.DamlException(
                  interpretation.Error
                    .FailedAuthorization(_, CreateMissingAuthorization(_, _, _, _))
                ),
                _,
              )
            ) =>
      }
    }

    "ok (Bob signed contract; Alice exercised Choice)" in {
      val command: ApiCommand =
        ApiCommand.Exercise(
          TemplateOrInterface.Template("T1"),
          toContractId("t1b"),
          "Choice1",
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty("Bob")),
              (Some[Name]("party2"), ValueParty("Alice")),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
  }

  "Exercise (within exercise)" - {

    // Test that an inner exercise has only the authorization of the signatories and
    // controllers; with no implicit authorization of signatories of the outer exercise.

    // TEST_EVIDENCE: Authorization: badly-authorized exercise/exercise (no implicit authority from outer exercise) is rejected
    "fail (no implicit authority from outer exercise's contract's signatories)" in {
      val command: ApiCommand =
        ApiCommand.Exercise(
          TemplateOrInterface.Template("X1"),
          toContractId("x1b"),
          "ChoiceA",
          ValueRecord(
            Some("ChoiceA"),
            ImmArray(
              (Some("cid"), ValueContractId(toContractId("x1c"))),
              (Some("controllerA"), ValueParty("Alice")),
              (
                Some("controllersB"),
                ValueList(
                  FrontStack(
                    ValueParty("Alice")
                  )
                ),
              ),
              (Some("party1"), ValueParty("Alice")),
              (Some("party2"), ValueParty("Bob")),
              (Some("party3"), ValueParty("Charlie")),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      inside(interpretResult) {
        case Left(
              engine.Error.Interpretation(
                engine.Error.Interpretation.DamlException(
                  interpretation.Error
                    .FailedAuthorization(_, CreateMissingAuthorization(_, _, _, _))
                ),
                _,
              )
            ) =>
      }
    }

    // TEST_EVIDENCE: Authorization: well-authorized exercise/exercise is accepted
    "ok" in {
      val command: ApiCommand =
        ApiCommand.Exercise(
          TemplateOrInterface.Template("X1"),
          toContractId("x1b"),
          "ChoiceA",
          ValueRecord(
            Some("ChoiceA"),
            ImmArray(
              (Some("cid"), ValueContractId(toContractId("x1c"))),
              (Some("controllerA"), ValueParty("Alice")),
              (
                Some("controllersB"),
                ValueList(
                  FrontStack(
                    ValueParty("Alice"),
                    // Adding Bob as an explicit controller of the inner exercise make the Authorization check pass again:
                    ValueParty("Bob"),
                  )
                ),
              ),
              (Some("party1"), ValueParty("Alice")),
              (Some("party2"), ValueParty("Bob")),
              (Some("party3"), ValueParty("Charlie")),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set("Alice"),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
  }
}
