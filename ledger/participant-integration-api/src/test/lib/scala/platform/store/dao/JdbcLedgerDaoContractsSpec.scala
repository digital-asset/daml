// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.utils.ErrorDetails
import com.daml.lf.data.Time.Timestamp

import java.util.UUID
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.value.Value.{ContractId, ValueText, VersionedContractInstance}
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.jdk.CollectionConverters._

private[dao] trait JdbcLedgerDaoContractsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def contractsReader = ledgerDao.contractsReader

  behavior of "JdbcLedgerDao (contracts)"

  it should "be able to persist and load contracts" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- contractsReader.lookupActiveContractAndLoadArgument(
        Set(alice),
        nonTransient(tx).loneElement,
      )
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.copy(agreementText = ""))
    }
  }

  it should "allow to divulge a contract that has already been committed" in {
    for {
      (_, tx) <- store(singleCreate)
      create = nonTransient(tx).loneElement
      _ <- storeCommitedContractDivulgence(
        id = create,
        divulgees = Set(charlie),
      )
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie), create)
    } yield {
      // The agreement text is always empty when retrieved from the contract store
      result shouldEqual Some(someVersionedContractInstance.copy(agreementText = ""))
    }
  }

  it should "not find contracts that are not visible to the requester" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- contractsReader.lookupActiveContractAndLoadArgument(
        Set(charlie),
        nonTransient(tx).loneElement,
      )
    } yield {
      result shouldEqual None
    }
  }

  it should "not find contracts that are not visible to any of the requesters" in {
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = None,
      )
      contractId = nonTransient(tx).loneElement
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie, emma), contractId)
    } yield {
      result shouldBe None
    }
  }

  it should "find contract if at least one of requesters is a stakeholder" in {
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = None,
      )
      contractId = nonTransient(tx).loneElement
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(charlie, emma), contractId)
    } yield {
      result.value shouldBe a[VersionedContractInstance]
    }
  }

  it should "find contract if at least one of requesters is a divulgee" in {
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = None,
      )
      contractId = nonTransient(tx).loneElement
      _ <- storeCommitedContractDivulgence(
        id = contractId,
        divulgees = Set(emma),
      )
      result <- contractsReader.lookupActiveContractAndLoadArgument(Set(david, emma), contractId)
    } yield {
      result.value shouldBe a[VersionedContractInstance]
    }
  }

  it should "not find keys if none of requesters are stakeholders" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    for {
      (_, _) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      result <- contractsReader.lookupContractKey(key, Set(charlie, emma))
    } yield {
      result shouldBe None
    }
  }

  it should "find a key if at least one of requesters is a stakeholder" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      contractId = nonTransient(tx).loneElement
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      result <- contractsReader.lookupContractKey(key, Set(emma, charlie))
    } yield {
      result.value shouldBe contractId
    }
  }

  it should "not find a key if the requesters are only divulgees" in {
    val aTextValue = ValueText(scala.util.Random.nextString(10))
    for {
      (_, tx) <- createAndStoreContract(
        submittingParties = Set(alice),
        signatories = Set(alice, bob),
        stakeholders = Set(alice, bob, charlie),
        key = Some(KeyWithMaintainers(aTextValue, Set(alice, bob))),
      )
      _ <- storeCommitedContractDivulgence(
        id = nonTransient(tx).loneElement,
        divulgees = Set(david, emma),
      )
      key = GlobalKey.assertBuild(someTemplateId, aTextValue)
      result <- contractsReader.lookupContractKey(key, Set(david, emma))
    } yield {
      result shouldBe None
    }
  }

  it should "prevent retrieving the maximum ledger time if some contracts are not found" in {
    val randomContractId = ContractId.assertFromString(s"#random-${UUID.randomUUID}")
    for {
      failure <- contractsReader.lookupMaximumLedgerTime(Set(randomContractId)).failed
    } yield {
      failure shouldBe a[StatusRuntimeException]
      assertIsLedgerTimeLookupError(failure, randomContractId.coid)
    }
  }

  it should "allow the retrieval of the maximum ledger time" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- contractsReader.lookupMaximumLedgerTime(nonTransient(tx))
    } yield {
      inside(result) { case Some(time) =>
        time should be <= Timestamp.now()
      }
    }
  }

  it should "allow the retrieval of the maximum ledger time even when there are divulged contracts" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")
    for {
      // Some contract divulged (its create node was not witnessed by any party on this participant)
      (_, _) <- storeCommitedContractDivulgence(
        id = divulgedContractId,
        divulgees = Set(charlie),
      )
      // An unrelated create node witnessed by some party on this participant
      (_, tx) <- store(singleCreate)
      contractIds = nonTransient(tx) + divulgedContractId
      result <- contractsReader.lookupMaximumLedgerTime(contractIds)
    } yield {
      inside(result) { case Some(tx.ledgerEffectiveTime) =>
        succeed
      }
    }
  }

  it should "allow the retrieval of the maximum ledger time when a local contract is also divulged" in {
    for {
      // Create node witnessed by some party on this participant
      (_, tx) <- store(singleCreate)
      contractId = nonTransient(tx).loneElement
      // Contract divulged to some other party on this participant
      (_, _) <- storeCommitedContractDivulgence(
        id = contractId,
        divulgees = Set(charlie),
      )
      result <- contractsReader.lookupMaximumLedgerTime(Set(contractId))
    } yield {
      inside(result) { case Some(tx.ledgerEffectiveTime) =>
        succeed
      }
    }
  }

  it should "allow the retrieval of the maximum ledger time even when there are only divulged contracts" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")
    for {
      // Some contract divulged (its create node was not witnessed by any party on this participant)
      (_, _) <- storeCommitedContractDivulgence(
        id = divulgedContractId,
        divulgees = Set(charlie),
      )
      result <- contractsReader.lookupMaximumLedgerTime(Set(divulgedContractId))
    } yield {
      result shouldBe None
    }
  }

  it should "not allow the retrieval of the maximum ledger time of archived divulged contracts" in {
    for {
      // Create node witnessed by some party on this participant
      (_, tx) <- store(singleCreate)
      divulgedContractId = nonTransient(tx).loneElement
      // Contract divulged to some other party on this participant
      (_, _) <- storeCommitedContractDivulgence(
        id = divulgedContractId,
        divulgees = Set(charlie),
      )
      // Consuming exercise node witnessed by some party on this participant
      _ <- store(singleExercise(divulgedContractId))
      failure <- contractsReader.lookupMaximumLedgerTime(Set(divulgedContractId)).failed
    } yield {
      failure shouldBe a[StatusRuntimeException]
      assertIsLedgerTimeLookupError(failure, divulgedContractId.coid)
    }
  }

  it should "store contracts with a transient contract in the global divulgence" in {
    store(fullyTransientWithChildren).flatMap(_ => succeed)
  }

  private[this] def assertIsLedgerTimeLookupError(exception: Throwable, missingContractId: String) =
    exception match {
      case ex: StatusRuntimeException =>
        val expectedErrorCodeId = LedgerApiErrors.InterpreterErrors.LookupErrors.ContractNotFound
        val status = StatusProto.fromThrowable(ex)
        val details = status.getDetailsList.asScala.toSeq

        status.getCode shouldBe expectedErrorCodeId.category.grpcCode.get.value()

        ErrorDetails.from(details) should contain theSameElementsAs Set(
          ErrorDetails.ResourceInfoDetail("CONTRACT_ID", missingContractId),
          ErrorDetails.ErrorInfoDetail(expectedErrorCodeId.id),
        )

      case _ => fail("Expected StatusRuntimeException")
    }
}
