// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsIntegrity extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  private val time1 = Timestamp.now()
  private val time2 = time1.addMicros(10)
  private val time3 = time2.addMicros(10)
  private val time4 = time3.addMicros(10)
  private val time5 = time4.addMicros(10)
  private val time6 = time5.addMicros(10)
  private val time7 = time6.addMicros(10)

  behavior of "IntegrityStorageBackend"

  it should "find duplicate event ids" in {
    val updates = Vector(
      dtoCreate(offset(7), 7L, hashCid("#7")),
      dtoCreate(offset(7), 7L, hashCid("#7")), // duplicate id
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(7), 7L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))

    // Error message should contain the duplicate event sequential id
    failure.getMessage should include("7")
  }

  it should "find duplicate event ids with different offsets" in {
    val updates = Vector(
      dtoCreate(offset(6), 7L, hashCid("#7")),
      dtoCreate(offset(7), 7L, hashCid("#7")), // duplicate id
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(7), 7L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))

    // Error message should contain the duplicate event sequential id
    failure.getMessage should include("7")
  }

  it should "find non-consecutive event ids" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, hashCid("#1")),
      dtoCreate(offset(3), 3L, hashCid("#3")), // non-consecutive id
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(3), 3L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))

    failure.getMessage should include("consecutive")

  }

  it should "not find non-consecutive event ids if those gaps are before the pruning offset" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, hashCid("#1")),
      dtoCreate(offset(3), 3L, hashCid("#3")), // non-consecutive id but after pruning offset
      dtoCreate(offset(4), 4L, hashCid("#4")),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(4), 4L))
    executeSql(backend.integrity.onlyForTestingVerifyIntegrity())
  }

  it should "detect monotonicity violation of record times for one domain in created table" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoCreate(
        offset(3),
        3L,
        hashCid("#3"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
      ),
      dtoCreate(
        offset(4),
        4L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        5L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one domain: offsets Offset(Bytes(000000000000000003)),Offset(Bytes(000000000000000005))"
    )
  }

  it should "detect monotonicity violation of record times for one domain in consuming exercise table" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoExercise(
        offset(3),
        3L,
        consuming = true,
        hashCid("#3"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
      ),
      dtoCreate(
        offset(4),
        4L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        5L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one domain: offsets Offset(Bytes(000000000000000003)),Offset(Bytes(000000000000000005))"
    )
  }

  it should "detect monotonicity violation of record times for one domain in non-consuming exercise table" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoExercise(
        offset(3),
        3L,
        consuming = false,
        hashCid("#3"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
      ),
      dtoCreate(
        offset(4),
        4L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        5L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one domain: offsets Offset(Bytes(000000000000000003)),Offset(Bytes(000000000000000005))"
    )
  }

  it should "detect monotonicity violation of record times for one domain in assign table" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoAssign(
        offset(3),
        3L,
        hashCid("#3"),
        targetDomainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
      ),
      dtoCreate(
        offset(4),
        4L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        5L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one domain: offsets Offset(Bytes(000000000000000003)),Offset(Bytes(000000000000000005))"
    )
  }

  it should "detect monotonicity violation of record times for one domain in unassign table" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoUnassign(
        offset(3),
        3L,
        hashCid("#3"),
        sourceDomainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
      ),
      dtoCreate(
        offset(4),
        4L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        5L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one domain: offsets Offset(Bytes(000000000000000003)),Offset(Bytes(000000000000000005))"
    )
  }

  it should "detect monotonicity violation of record times for one domain in completions table" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoCompletion(
        offset(3),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
      ),
      dtoCreate(
        offset(4),
        3L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        4L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one domain: offsets Offset(Bytes(000000000000000003)),Offset(Bytes(000000000000000005))"
    )
  }

  it should "not detect monotonicity violation of record times for one domain in completions table, if it is a timely-reject" in {
    val updates = Vector(
      dtoCreate(
        offset(1),
        1L,
        hashCid("#1"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time5,
      ),
      dtoCreate(
        offset(2),
        2L,
        hashCid("#2"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time1,
      ),
      dtoCompletion(
        offset(3),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time7,
        messageUuid = Some("message uuid"),
      ),
      dtoCreate(
        offset(4),
        3L,
        hashCid("#4"),
        domainId = someDomainId2.toProtoPrimitive,
        recordTime = time3,
      ),
      dtoCreate(
        offset(5),
        4L,
        hashCid("#5"),
        domainId = someDomainId.toProtoPrimitive,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    executeSql(backend.integrity.onlyForTestingVerifyIntegrity())
  }

  it should "detect duplicated update ids" in {
    val updates = Vector(
      dtoTransactionMeta(
        offset(1),
        1L,
        4L,
        transactionId = Some(transactionIdFromOffset(offset(1))),
      ),
      dtoTransactionMeta(
        offset(2),
        1L,
        4L,
        transactionId = Some(transactionIdFromOffset(offset(2))),
      ),
      dtoTransactionMeta(
        offset(3),
        1L,
        4L,
        transactionId = Some(transactionIdFromOffset(offset(2))),
      ),
      dtoTransactionMeta(
        offset(4),
        1L,
        4L,
        transactionId = Some(transactionIdFromOffset(offset(4))),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of duplicate update ID [000000000000000002] found for offsets Offset(Bytes(000000000000000002)), Offset(Bytes(000000000000000003))"
    )
  }

  it should "detect duplicated completion offsets" in {
    val updates = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset(2)
      ),
      dtoCompletion(
        offset(2)
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "occurrence of duplicate offset found for lapi_command_completions: for offset Offset(Bytes(000000000000000002)) 2 rows found"
    )
  }

  it should "detect same completion entries for different offsets" in {
    val updates = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset(2),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        transactionId = Some(transactionIdFromOffset(offset(2))),
      ),
      dtoCompletion(
        offset(3),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        transactionId = Some(transactionIdFromOffset(offset(2))),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.onlyForTestingVerifyIntegrity()))
    failure.getMessage should include(
      "duplicate entries found in lapi_command_completions at offsets (first 10 shown) List(Offset(Bytes(000000000000000002)), Offset(Bytes(000000000000000003)))"
    )
  }

  it should "not find errors beyond the ledger end" in {
    val updates = Vector(
      dtoCreate(offset(1), 1L, hashCid("#1")),
      dtoCreate(offset(2), 2L, hashCid("#2")),
      dtoCreate(offset(7), 7L, hashCid("#7")), // beyond the ledger end
      dtoCreate(offset(7), 7L, hashCid("#7")), // duplicate id (beyond ledger end)
      dtoCreate(offset(9), 9L, hashCid("#9")), // non-consecutive id (beyond ledger end)
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    executeSql(backend.integrity.onlyForTestingVerifyIntegrity())

    // Succeeds if verifyIntegrity() doesn't throw
    succeed
  }
}
