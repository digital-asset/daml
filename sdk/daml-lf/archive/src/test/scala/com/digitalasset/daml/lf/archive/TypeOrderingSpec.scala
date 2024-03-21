// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.DamlLf1
import com.daml.lf.language.{Ast, TypeOrdering}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class TypeOrderingSpec extends AnyWordSpec with Matchers {

  "TypeOrdering" should {

    "follow archive protobuf order" in {

      val protoMapping =
        DecodeV1.builtinTypeInfos.iterator.map(info => info.proto -> info.bTyp).toMap

      val primTypesInProtoOrder =
        DamlLf1.PrimType.getDescriptor.getValues.asScala
          .map(desc => DamlLf1.PrimType.internalGetValueMap().findValueByNumber(desc.getNumber))
          .sortBy(_.getNumber)
          .collect(protoMapping)

      primTypesInProtoOrder.sortBy(Ast.TBuiltin)(
        TypeOrdering.compare
      ) shouldBe primTypesInProtoOrder
    }
  }

}
