// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.lf.transaction.Versioned
import com.daml.lf.value.Value
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Salt, TestSalt}
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{DomainId, MediatorId, MediatorRef, UniqueIdentifier}
import com.digitalasset.canton.util.LfTransactionBuilder.defaultTemplateId
import com.digitalasset.canton.{BaseTest, LfPartyId, protocol}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.UUID

class SerializableContractAuthenticatorImplTest extends AnyWordSpec with BaseTest {

  classOf[SerializableContractAuthenticatorImpl].getSimpleName when {
    "provided with aAuthenticatedContractIdVersionV2 authenticator" should {
      "using a AuthenticatedContractIdVersionV2 contract id" should {
        "correctly authenticate the contract" in new WithContractAuthenticator(
          AuthenticatedContractIdVersionV2
        ) {
          contractAuthenticator.authenticate(contract) shouldBe Right(())
        }
      }

      "using a generic contract id" should {
        "correctly authenticate the contract" in new WithContractAuthenticator(
          AuthenticatedContractIdVersionV2
        ) {
          val nonAuthenticatedContractId = ExampleTransactionFactory.suffixedId(1, 2)
          contractAuthenticator.authenticate(
            contract.copy(contractId = nonAuthenticatedContractId)
          ) shouldBe Right(())
        }
      }
    }

    "provided with a AuthenticatedContractIdVersionV2 authenticator" should {
      "using a contract with a mismatching salt" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
          val changedSalt = TestSalt.generateSalt(1337)
          testFailedAuthentication(
            _.copy(contractSalt = Some(changedSalt)),
            testedSalt = changedSalt,
          )
        }
      }

      "using a AuthenticatedContractIdVersionV2 with a missing salt" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
          contractAuthenticator.authenticate(contract.copy(contractSalt = None)) shouldBe Left(
            s"Contract salt missing in serializable contract with authenticating contract id ($contractId)"
          )
        }
      }

      "using a contract with changed ledger time" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
          val changedLedgerTime = ledgerTime.add(Duration.ofDays(1L))
          testFailedAuthentication(
            _.copy(ledgerCreateTime = LedgerCreateTime(changedLedgerTime)),
            testedLedgerTime = changedLedgerTime,
          )
        }
      }

      "using a contract with changed contents" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
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
      }
    }

    "provided with a AuthenticatedContractIdVersionV2 authenticator" should {
      "using a contract that has mismatching signatories" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
          val changedSignatories = signatories - alice
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
      }

      "using a contract that has mismatching observers" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
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
      }

      "using a contract with a missing contract key" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
          testFailedAuthentication(
            contract =>
              contract.copy(metadata =
                ContractMetadata
                  .tryCreate(contract.metadata.signatories, contract.metadata.stakeholders, None)
              ),
            testedContractKey = None,
          )
        }
      }

      "using a contract that has a mismatching contract key hash" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
          val changedContractKey = ExampleTransactionFactory.globalKeyWithMaintainers(
            key = LfGlobalKey.assertBuild(defaultTemplateId, Value.ValueInt64(0)),
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
      }

      "using a contract that has mismatching contract key maintainers" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
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
      }

      "using a contract that has missing contract key maintainers" should {
        "fail authentication" in new WithContractAuthenticator(AuthenticatedContractIdVersionV2) {
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
    }
  }
}

class WithContractAuthenticator(contractIdVersion: CantonContractIdVersion) extends BaseTest {
  protected lazy val unicumGenerator = new UnicumGenerator(new SymbolicPureCrypto())
  protected lazy val contractAuthenticator = new SerializableContractAuthenticatorImpl(
    unicumGenerator
  )

  protected lazy val contractInstance = ExampleTransactionFactory.contractInstance()
  protected lazy val ledgerTime = CantonTimestamp.MinValue
  protected lazy val alice = LfPartyId.assertFromString("alice")
  protected lazy val signatories = Set(ExampleTransactionFactory.signatory, alice)
  protected lazy val observers = Set(ExampleTransactionFactory.observer)
  protected lazy val maintainers = Set(ExampleTransactionFactory.signatory)
  protected lazy val contractKey =
    ExampleTransactionFactory.globalKeyWithMaintainers(maintainers = maintainers)
  protected lazy val contractMetadata =
    ContractMetadata.tryCreate(signatories, signatories ++ observers, Some(contractKey))
  protected lazy val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
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
      .recomputeUnicum(
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
    contractAuthenticator.authenticate(changeContract(contract)) shouldBe Left(
      s"Mismatching contract id suffixes. expected: $expectedSuffix vs actual: $actualSuffix"
    )
  }
}
