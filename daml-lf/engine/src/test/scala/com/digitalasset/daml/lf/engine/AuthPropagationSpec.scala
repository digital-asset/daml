// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.command.{ApiCommand, Commands, CreateCommand, ExerciseCommand}
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref.{
  Name,
  Party,
  ParticipantId,
  PackageId,
  Identifier,
  QualifiedName,
  ChoiceName,
}
import com.daml.lf.data.Time
import com.daml.lf.data.{ImmArray, Bytes}
import com.daml.lf.language.Ast.Package
import com.daml.lf.ledger.FailedAuthorization.{
  CreateMissingAuthorization,
  ExerciseMissingAuthorization,
}
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.transaction.Transaction.Metadata
import com.daml.lf.transaction.{SubmittedTransaction, TransactionVersion}
import com.daml.lf.value.Value.{
  ContractId,
  ValueRecord,
  ValueParty,
  VersionedContractInstance,
  ValueContractId,
  ValueList,
}

import java.io.File

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class AuthPropagationSpec extends AnyFreeSpec with Matchers with Inside with BazelRunfiles {

  implicit private def toName(s: String): Name = Name.assertFromString(s)

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

  private val choice1name: ChoiceName = ChoiceName.assertFromString("Choice1")
  private val choiceAname: ChoiceName = ChoiceName.assertFromString("ChoiceA")

  private val alice: Party = Party.assertFromString("Alice")
  private val bob: Party = Party.assertFromString("Bob")
  private val charlie: Party = Party.assertFromString("Charlie")

  private def t1InstanceFor(party: Party): VersionedContractInstance = {
    VersionedContractInstance(
      TransactionVersion.VDev,
      "T1",
      ValueRecord(
        Some("T1"),
        ImmArray((Some[Name]("party"), ValueParty(party))),
      ),
      "",
    )
  }

  private def x1InstanceFor(party: Party): VersionedContractInstance = {
    VersionedContractInstance(
      TransactionVersion.VDev,
      "X1",
      ValueRecord(
        Some("X1"),
        ImmArray((Some[Name]("party"), ValueParty(party))),
      ),
      "",
    )
  }

  private val defaultContracts: Map[ContractId, VersionedContractInstance] =
    Map(
      toContractId("t1a") -> t1InstanceFor(alice),
      toContractId("t1b") -> t1InstanceFor(bob),
      toContractId("x1b") -> x1InstanceFor(bob),
      toContractId("x1c") -> x1InstanceFor(charlie),
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
          Commands(ImmArray(command), let, "commands-tag"),
          participant,
          submissionSeed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

    interpretResult
  }

  "Create(T1)" - {
    val command: ApiCommand =
      CreateCommand(
        "T1",
        ValueRecord(
          Some("T1"),
          ImmArray(
            (Some[Name]("party"), ValueParty(alice))
          ),
        ),
      )
    "ok" in {
      val interpretResult = go(
        submitters = Set(alice),
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
      CreateCommand(
        "T2",
        ValueRecord(
          Some("T2"),
          ImmArray(
            (Some[Name]("party1"), ValueParty(alice)),
            (Some[Name]("party2"), ValueParty(bob)),
          ),
        ),
      )

    "ok" in {
      val interpretResult = go(
        submitters = Set(alice, bob),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
    "fail" in {
      val interpretResult = go(
        submitters = Set(alice),
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

    "ok (Alice signed contract; Bob exercised Choice)" in {
      val command: ApiCommand =
        ExerciseCommand(
          "T1",
          toContractId("t1a"),
          choice1name,
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty(alice)),
              (Some[Name]("party2"), ValueParty(bob)),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set(bob),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }

    "fail: ExerciseMissingAuthorization" in {
      val command: ApiCommand =
        ExerciseCommand(
          "T1",
          toContractId("t1a"),
          choice1name,
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty(alice)),
              (Some[Name]("party2"), ValueParty(bob)),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set(alice),
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

    "fail: CreateMissingAuthorization" in {
      val command: ApiCommand =
        ExerciseCommand(
          "T1",
          toContractId("t1a"),
          choice1name,
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty(bob)),
              (Some[Name]("party2"), ValueParty(alice)),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set(alice),
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
        ExerciseCommand(
          "T1",
          toContractId("t1b"),
          choice1name,
          ValueRecord(
            Some("Choice1"),
            ImmArray(
              (Some[Name]("party1"), ValueParty(bob)),
              (Some[Name]("party2"), ValueParty(alice)),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set(alice),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
  }

  "Exercise (within exercise)" - {

    "fail (no implicit authority from outer exercise's contract's signatories)" in {
      val command: ApiCommand =
        ExerciseCommand(
          "X1",
          toContractId("x1b"),
          choiceAname,
          ValueRecord(
            Some("ChoiceA"),
            ImmArray(
              (Some("cid"), ValueContractId(toContractId("x1c"))),
              (Some("controllerA"), ValueParty(alice)),
              (
                Some("controllersB"),
                ValueList(
                  FrontStack(
                    ValueParty(alice)
                  )
                ),
              ),
              (Some("party1"), ValueParty(alice)),
              (Some("party2"), ValueParty(bob)),
              (Some("party3"), ValueParty(charlie)),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set(alice),
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

    "ok" in {
      val command: ApiCommand =
        ExerciseCommand(
          "X1",
          toContractId("x1b"),
          choiceAname,
          ValueRecord(
            Some("ChoiceA"),
            ImmArray(
              (Some("cid"), ValueContractId(toContractId("x1c"))),
              (Some("controllerA"), ValueParty(alice)),
              (
                Some("controllersB"),
                ValueList(
                  FrontStack(
                    ValueParty(alice),
                    ValueParty(bob), // bob must be an explicit controller on inner exercise
                  )
                ),
              ),
              (Some("party1"), ValueParty(alice)),
              (Some("party2"), ValueParty(bob)),
              (Some("party3"), ValueParty(charlie)),
            ),
          ),
        )
      val interpretResult = go(
        submitters = Set(alice),
        command = command,
      )
      interpretResult shouldBe a[Right[_, _]]
    }
  }
}
