// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.{ErrorInfoDetail, RequestInfoDetail}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, Assertion, OptionValues}

import scala.jdk.CollectionConverters.*

trait ErrorsAssertions extends Matchers with OptionValues with AppendedClues {

  /** NOTE: This method is not suitable for:
    * 1) security sensitive error codes (e.g. internal or authentication related) as they are stripped from all the details when being converted to instances of [[StatusRuntimeException]],
    * 2) error codes that do not translate to gRPC level errors (i.e. error codes that don't have a corresponding gRPC status)
    */
  def assertMatchesErrorCode(
      actual: StatusRuntimeException,
      expectedErrorCode: ErrorCode,
  ): Assertion = {
    val actualErrorCodeId = ErrorDetails.from(actual).collectFirst {
      case ErrorInfoDetail(errorCodeId, _) => errorCodeId
    }
    val actualDescription = Option(actual.getStatus.getDescription)
    val actualStatusCode = actual.getStatus.getCode
    val cp = new Checkpoint
    cp { actualErrorCodeId.value shouldBe expectedErrorCode.id }
    cp { Some(actualStatusCode) shouldBe expectedErrorCode.category.grpcCode }
    cp { actualDescription.value should startWith(expectedErrorCode.id) }
    cp.reportAll()
    succeed
  }

  def assertStatus(
      actual: com.google.rpc.Status,
      expected: com.google.rpc.Status,
  ): Assertion = {
    val actualDetails = ErrorDetails.from(actual)
    val expectedDetails = ErrorDetails.from(expected)
    val actualDescription = Option(actual.getMessage)
    val expectedDescription = Option(expected.getMessage)
    val actualStatusCode = actual.getCode
    val expectedStatusCode = expected.getCode
    val cp = new Checkpoint
    cp { actualDescription shouldBe expectedDescription }
    cp {
      actualStatusCode shouldBe expectedStatusCode withClue (s", expecting status code: '${expectedStatusCode}''")
    }
    cp { actualDetails should contain theSameElementsAs expectedDetails }
    cp.reportAll()
    succeed
  }

  /** Asserts that the two errors have the same code, message and details.
    */
  def assertError(
      actual: StatusRuntimeException,
      expected: StatusRuntimeException,
  ): Unit = {
    val expectedStatus = StatusProto.fromThrowable(expected)
    val expectedDetails = expectedStatus.getDetailsList.asScala.toSeq
    assertError(
      actual = actual,
      expectedStatusCode = expected.getStatus.getCode,
      expectedMessage = expectedStatus.getMessage,
      expectedDetails = ErrorDetails.from(expectedDetails),
    )
  }

  /** @param verifyEmptyStackTrace - should be enabled for the server-side testing and disabled for the client side testing
    */
  def assertError(
      actual: StatusRuntimeException,
      expectedStatusCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      verifyEmptyStackTrace: Boolean = true,
  ): Unit = {
    val actualStatus = StatusProto.fromThrowable(actual)
    val actualDetails = actualStatus.getDetailsList.asScala.toSeq
    val cp = new Checkpoint
    cp { actual.getStatus.getCode shouldBe expectedStatusCode }
    cp { actualStatus.getMessage shouldBe expectedMessage }
    cp {
      ErrorDetails.from(actualDetails) should contain theSameElementsAs expectedDetails
    }
    if (verifyEmptyStackTrace) {
      cp {
        actual.getStackTrace.length shouldBe 0 withClue ("it should contain no stacktrace")
      }
    }
    cp { actual.getCause shouldBe null }
    cp.reportAll()
  }

  /** Asserts that the error has the expected code and matches the form of the message and details.
    */
  def assertError(
      actual: StatusRuntimeException,
      expectedStatusCode: Code,
      expectedMessage: String => String,
      expectedDetails: String => String => Seq[ErrorDetails.ErrorDetail],
  ): Unit = {
    val actualStatus = StatusProto.fromThrowable(actual)
    val actualDetails = actualStatus.getDetailsList.asScala.toSeq
    val errorDetails = ErrorDetails.from(actualDetails)
    val tid = errorDetails.collectFirst({ case RequestInfoDetail(tid) => tid }).value
    val errorInfoDetail =
      errorDetails.collectFirst({ case detail: ErrorInfoDetail => detail }).value
    val submissionId = errorInfoDetail.metadata.get("submissionId").value
    assertError(
      actual,
      expectedStatusCode,
      expectedMessage(tid),
      expectedDetails(tid)(submissionId),
      false,
    )

  }

}
