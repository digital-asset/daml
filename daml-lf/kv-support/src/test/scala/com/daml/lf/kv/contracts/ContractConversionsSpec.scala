// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.contracts

import com.daml.lf.data.Ref
import com.daml.lf.kv.ConversionError
import com.daml.lf.transaction.{TransactionOuterClass, TransactionVersion, Versioned}
import com.daml.lf.value.{Value, ValueOuterClass}
import com.google.protobuf
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractConversionsSpec extends AnyWordSpec with Matchers {
  import ContractConversionsSpec._

  "encodeContractInstance" should {
    "successfully encode a contract instance" in {
      ContractConversions.encodeContractInstance(aContractInstance) shouldBe Right(
        aRawContractInstance
      )
    }
  }

  "decodeContractInstance" should {
    "successfully decode a contract instance" in {
      ContractConversions.decodeContractInstance(aRawContractInstance) shouldBe Right(
        aContractInstance
      )
    }

    "fail on a broken contract instance" in {
      ContractConversions.decodeContractInstance(
        RawContractInstance(ByteString.copyFromUtf8("wrong"))
      ) shouldBe Left(ConversionError.ParseError("Protocol message tag had invalid wire type."))
    }
  }
}

object ContractConversionsSpec {
  private val aDummyName = "dummyName"
  private val aModuleName = "DummyModule"
  private val aPackageId = "-dummyPkg-"
  private val aAgreement = "agreement"

  private val aContractInstance = Versioned(
    version = TransactionVersion.VDev,
    Value.ContractInstanceWithAgreement(
      Value.ContractInstance(
        template = Ref.Identifier(
          Ref.PackageId.assertFromString(aPackageId),
          Ref.QualifiedName.assertFromString(s"$aModuleName:$aDummyName"),
        ),
        arg = Value.ValueUnit,
      ),
      agreementText = aAgreement,
    ),
  )

  private val aRawContractInstance = RawContractInstance(
    TransactionOuterClass.ContractInstance
      .newBuilder()
      .setTemplateId(
        ValueOuterClass.Identifier
          .newBuilder()
          .setPackageId(aPackageId)
          .addModuleName(aModuleName)
          .addName(aDummyName)
      )
      .setAgreement(aAgreement)
      .setArgVersioned(
        ValueOuterClass.VersionedValue
          .newBuilder()
          .setValue(
            ValueOuterClass.Value
              .newBuilder()
              .setUnit(protobuf.Empty.newBuilder())
              .build()
              .toByteString
          )
          .setVersion(TransactionVersion.VDev.protoValue)
          .build()
      )
      .build()
      .toByteString
  )
}
