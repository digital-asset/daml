// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.lf.data.Ref
import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, TestSalt}
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator.AuthenticationPurpose
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DomainId, MediatorId, MediatorRef, UniqueIdentifier}
import com.digitalasset.canton.util.LfTransactionBuilder.{defaultKeyPackageName, defaultTemplateId}
import com.digitalasset.canton.{BaseTest, LfPartyId, protocol}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID

class SerializableContractAuthenticatorImplTest extends AnyWordSpec with BaseTest {
  "authenticate" when {
    for (
      contractIdVersion <- Seq(
        NonAuthenticatedContractId,
        AuthenticatedContractIdV1,
        AuthenticatedContractIdV2,
        AuthenticatedContractIdV3,
      )
    ) {
      s"provided with a contract-id with version $contractIdVersion" should {
        `tests for all contract id versions`(contractIdVersion)

        `tests only for authenticated contract ids`(contractIdVersion)

        `tests only for contract id versions gteq V2`(contractIdVersion)

        `tests only for contract id versions gteq V3`(contractIdVersion)
      }
    }
  }

  private def `tests for all contract id versions`(
      contractIdVersion: CantonContractIdVersion
  ): Unit = {
    "correctly authenticate the contract" in new WithContractAuthenticator(contractIdVersion) {
      contractAuthenticator.authenticateInputContract(contract) shouldBe (
        if (contractIdVersion.isAuthenticated) Right(())
        else Left("Contract id scheme does not support authentication: NonAuthenticatedContractId")
      )
    }

    "crash with an exception on unknown contract-id" in new WithContractAuthenticator(
      contractIdVersion
    ) {
      private val unknownContractId = ExampleTransactionFactory.suffixedId(1, 2)

      intercept[IllegalArgumentException] {
        contractAuthenticator.authenticateInputContract(
          contract.copy(contractId = unknownContractId)
        )
      }.getMessage should include(
        "Unsupported contract id scheme detected. Please contact support"
      )
    }
  }

  private def `tests only for authenticated contract ids`(
      contractIdVersion: CantonContractIdVersion
  ): Unit =
    if (contractIdVersion.isAuthenticated) {
      "fail authentication on missing salt" in new WithContractAuthenticator(contractIdVersion) {
        contractAuthenticator.authenticateInputContract(
          contract.copy(contractSalt = None)
        ) shouldBe Left(
          s"Contract salt missing in serializable contract with authenticating contract id ($contractId)"
        )
      }

      "fail authentication when using a contract with changed ledger time" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        val changedLedgerTime = ledgerTime.add(Duration.ofDays(1L))
        testFailedAuthentication(
          _.copy(ledgerCreateTime = LedgerCreateTime(changedLedgerTime)),
          testedLedgerTime = changedLedgerTime,
        )
      }

      "fail authentication when using a contract with changed contents" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        val changedContractInstance = ExampleTransactionFactory.contractInstance(
          Seq(ExampleTransactionFactory.suffixedId(1, 2))
        )
        testFailedAuthentication(
          _.copy(rawContractInstance =
            ExampleTransactionFactory.asSerializableRaw(changedContractInstance, "")
          ),
          testedContractInstance =
            ExampleTransactionFactory.asSerializableRaw(changedContractInstance, ""),
        )
      }

      Seq(
        AuthenticationPurpose.BasicContractAuthentication,
        AuthenticationPurpose.ContractUpgradeValidation,
      ) foreach { authenticationPurpose =>
        if (contractIdVersion >= authenticationPurpose.minimumContractIdVersion) {
          s"correctly authenticate the contract for contract id version $contractIdVersion and purpose $authenticationPurpose" in new WithContractAuthenticator(
            contractIdVersion
          ) {
            contractAuthenticator.authenticate(authenticationPurpose, contract) shouldBe Right(())
          }
        } else {
          s"reject authentication for contract id version $contractIdVersion and purpose $authenticationPurpose" in new WithContractAuthenticator(
            contractIdVersion
          ) {
            contractAuthenticator.authenticate(authenticationPurpose, contract) shouldBe Left(
              s"Authentication for ${authenticationPurpose.purposeDescription} requires at least contract id version ${authenticationPurpose.minimumContractIdVersion} instead of the provided $contractIdVersion"
            )
          }
        }
      }
    }

  private def `tests only for contract id versions gteq V2`(
      contractIdVersion: CantonContractIdVersion
  ): Unit =
    if (contractIdVersion >= AuthenticatedContractIdV2) {
      "fail authentication when using a contract that has mismatching signatories" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        private val changedSignatories = signatories - alice
        // As observers == stakeholders -- signatories, we also need to ensure that alice is not a stakeholder - or unicum's will be calculated incorrectly
        testFailedAuthentication(
          contract =>
            contract.copy(metadata =
              ContractMetadata.tryCreate(
                changedSignatories,
                contract.metadata.stakeholders - alice,
                contract.metadata.maybeKeyWithMaintainersVersioned,
              )
            ),
          testedSignatories = changedSignatories,
        )
      }

      "fail authentication when using a contract that has mismatching observers" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        val changedObservers = observers ++ Set(LfPartyId.assertFromString("observer::changed"))
        testFailedAuthentication(
          contract =>
            contract.copy(metadata =
              ContractMetadata.tryCreate(
                contract.metadata.signatories,
                contract.metadata.stakeholders ++ changedObservers,
                contract.metadata.maybeKeyWithMaintainersVersioned,
              )
            ),
          testedObservers = changedObservers,
        )
      }

      "fail authentication when using a contract with a missing contract key" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        testFailedAuthentication(
          contract =>
            contract.copy(metadata =
              ContractMetadata
                .tryCreate(contract.metadata.signatories, contract.metadata.stakeholders, None)
            ),
          testedContractKey = None,
        )
      }

      "fail authentication when using a contract that has a mismatching contract key hash" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        val changedContractKey = ExampleTransactionFactory.globalKeyWithMaintainers(
          key = LfGlobalKey
            .assertBuild(defaultTemplateId, Value.ValueInt64(7), defaultKeyPackageName),
          maintainers = maintainers,
        )
        testFailedAuthentication(
          contract =>
            contract.copy(metadata =
              ContractMetadata.tryCreate(
                contract.metadata.signatories,
                contract.metadata.stakeholders,
                Some(changedContractKey),
              )
            ),
          testedContractKey = Some(changedContractKey),
        )
      }

      "fail authentication when using a contract that has mismatching contract key maintainers" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        val changedContractKey =
          ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = Set(alice))
        testFailedAuthentication(
          contract =>
            contract.copy(metadata =
              ContractMetadata.tryCreate(
                contract.metadata.signatories,
                contract.metadata.stakeholders,
                Some(changedContractKey),
              )
            ),
          testedContractKey = Some(changedContractKey),
        )
      }

      "fail authentication when using a contract that has missing contract key maintainers" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        val changedContractKey =
          ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = Set.empty)
        testFailedAuthentication(
          contract =>
            contract.copy(metadata =
              ContractMetadata.tryCreate(
                contract.metadata.signatories,
                contract.metadata.stakeholders,
                Some(changedContractKey),
              )
            ),
          testedContractKey = Some(changedContractKey),
        )
      }
    }

  private def `tests only for contract id versions gteq V3`(
      contractIdVersion: CantonContractIdVersion
  ): Unit =
    if (contractIdVersion >= AuthenticatedContractIdV3) {
      "fail authentication when the contract package-name changes" in new WithContractAuthenticator(
        contractIdVersion
      ) {
        override protected def contractInstance: LfContractInst = ExampleTransactionFactory
          .contractInstance()
          .copy(version = LfTransactionVersion.VDev)
          .focus(_.unversioned.packageName)
          .replace(Some(Ref.PackageName.assertFromString("originalPkgName")))

        private val testedContractInstance = contractInstance
          .focus(_.unversioned.packageName)
          .replace(Some(Ref.PackageName.assertFromString("changedPkgName")))

        private val serializableRawContractInstance =
          ExampleTransactionFactory.asSerializableRaw(testedContractInstance, "")
        testFailedAuthentication(
          _.copy(rawContractInstance = serializableRawContractInstance),
          testedContractInstance = serializableRawContractInstance,
        )
      }
    }
}

class WithContractAuthenticator(contractIdVersion: CantonContractIdVersion) extends BaseTest {
  protected lazy val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())
  protected lazy val contractAuthenticator = new SerializableContractAuthenticatorImpl(
    unicumGenerator,
    false,
  )

  protected def contractInstance = ExampleTransactionFactory.contractInstance()

  protected lazy val ledgerTime = CantonTimestamp.MinValue
  protected lazy val alice = LfPartyId.assertFromString("alice")
  protected lazy val signatories = Set(ExampleTransactionFactory.signatory, alice)
  protected lazy val observers = Set(ExampleTransactionFactory.observer)
  protected lazy val maintainers = Set(ExampleTransactionFactory.signatory)
  protected lazy val contractKey =
    ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = maintainers)
  protected lazy val contractMetadata =
    ContractMetadata.tryCreate(signatories, signatories ++ observers, Some(contractKey))
  protected lazy val (contractSalt, unicum) = unicumGenerator
    .generateSaltAndUnicum(
      domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da")),
      mediator = MediatorRef(MediatorId(UniqueIdentifier.tryCreate("mediator", "other"))),
      transactionUuid = new UUID(1L, 1L),
      viewPosition = ViewPosition(List.empty),
      viewParticipantDataSalt = TestSalt.generateSalt(1),
      createIndex = 0,
      ledgerCreateTime = LedgerCreateTime(ledgerTime),
      metadata = contractMetadata,
      suffixedContractInstance =
        ExampleTransactionFactory.asSerializableRaw(contractInstance, agreementText = ""),
      contractIdVersion = contractIdVersion,
    )
    .value

  protected lazy val contractId = contractIdVersion.fromDiscriminator(
    ExampleTransactionFactory.lfHash(1337),
    unicum,
  )

  lazy val contract =
    SerializableContract(
      contractId = contractId,
      contractInstance = contractInstance,
      metadata = contractMetadata,
      ledgerTime = ledgerTime,
      contractSalt = Some(contractSalt.unwrap),
      unvalidatedAgreementText = AgreementText.empty,
    ).valueOrFail("Failed creating serializable contract instance")

  protected def testFailedAuthentication(
      changeContract: SerializableContract => SerializableContract,
      testedSalt: Salt = contractSalt.unwrap,
      testedLedgerTime: CantonTimestamp = ledgerTime,
      testedContractInstance: SerializableRawContractInstance =
        ExampleTransactionFactory.asSerializableRaw(contractInstance, ""),
      testedSignatories: Set[LfPartyId] = signatories,
      testedObservers: Set[LfPartyId] = observers,
      testedContractKey: Option[Versioned[protocol.LfGlobalKeyWithMaintainers]] = Some(contractKey),
  ): Assertion = {
    val recomputedUnicum = unicumGenerator
      .computeUnicum(
        contractSalt = testedSalt,
        ledgerCreateTime = LedgerCreateTime(testedLedgerTime),
        metadata = ContractMetadata
          .tryCreate(testedSignatories, testedSignatories ++ testedObservers, testedContractKey),
        suffixedContractInstance = testedContractInstance,
        contractIdVersion = contractIdVersion,
      )
      .valueOrFail("Failed unicum computation")
    val actualSuffix = unicum.toContractIdSuffix(contractIdVersion)
    val expectedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
    contractAuthenticator.authenticateInputContract(changeContract(contract)) shouldBe Left(
      s"Mismatching contract id suffixes. expected: $expectedSuffix vs actual: $actualSuffix"
    )
  }
}
