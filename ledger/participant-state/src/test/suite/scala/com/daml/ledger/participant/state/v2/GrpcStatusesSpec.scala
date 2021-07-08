package com.daml.ledger.participant.state.v2

import com.google.rpc.status.Status
import com.google.protobuf.any
import com.google.rpc.error_details.{ErrorInfo, RequestInfo}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GrpcStatusesSpec extends AnyWordSpec with Matchers {
  "completeWithOffset" should {
    "throw in case no error details can be found" in {
      assertThrows[IllegalArgumentException] {
        GrpcStatuses.completeWithOffset(Status.defaultInstance, aCompletionKey, aCompletionOffset)
      }
    }

    "throw in case no ErrorInfo message is available" in {
      val aMessage = RequestInfo.of("a", "b")
      val inputStatus = Status.of(123, "an error", Seq(any.Any.pack(aMessage)))
      assertThrows[IllegalArgumentException] {
        GrpcStatuses.completeWithOffset(inputStatus, aCompletionKey, aCompletionOffset)
      }
    }

    "update metadata for ErrorInfo with completion offset at completion key" in {
      val anErrorInfo = ErrorInfo.of("reason", "domain", Map("key" -> "value"))
      val inputStatus = Status.of(123, "an error", Seq(any.Any.pack(anErrorInfo)))

      GrpcStatuses.completeWithOffset(inputStatus, aCompletionKey, aCompletionOffset) should be(
        inputStatus.copy(details = Seq(
          any.Any.pack(anErrorInfo
            .copy(metadata = anErrorInfo.metadata + (aCompletionKey -> aCompletionOffset.toHexString))
          )
        ))
      )
    }
  }

  private lazy val aCompletionKey = "a key"
  private lazy val aCompletionOffset = Offset.fromByteArray(Array[Byte](1, 2))
}
