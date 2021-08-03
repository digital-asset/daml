// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform
package db.migration.translation

import com.daml.lf.value.ValueCoder
import com.daml.lf.value.{ValueOuterClass => proto}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class ValueSerializerTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "DeprecatedValueVersionsError" should {
    "extract deprecated version from unsupported value version error" in {

      val negativeTestCases = Table("version", "0", "6", "7", "11", "999", "some version")
      val positiveTestCases = Table("version", "1", "2", "3", "4", "5")
      val valueBuilder =
        proto.VersionedValue
          .newBuilder()
          .setValue(proto.Value.newBuilder().setBool(true).build().toByteString)

      forEvery(negativeTestCases) { version =>
        val x = ValueSerializer.DeprecatedValueVersionsError.unapply(
          ValueCoder.decodeValue(ValueCoder.CidDecoder, valueBuilder.setVersion(version).build())
        )
        x shouldBe None
      }

      forEvery(positiveTestCases) { version =>
        val x = ValueSerializer.DeprecatedValueVersionsError.unapply(
          ValueCoder.decodeValue(ValueCoder.CidDecoder, valueBuilder.setVersion(version).build())
        )
        x shouldBe Some(version)
      }

    }
  }

}
