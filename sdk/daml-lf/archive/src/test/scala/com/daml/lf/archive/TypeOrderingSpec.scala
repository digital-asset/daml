// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.daml.daml_lf_dev.DamlLf2
import com.daml.lf.language.{Ast, TypeOrdering}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class TypeOrderingSpec extends AnyWordSpec with Matchers {

  "TypeOrdering" should {

    "follow archive protobuf order" in {

      val protoMapping =
        DecodeV2.builtinTypeInfos.iterator.map(info => info.proto -> info.bTyp).toMap

      val primTypesInProtoOrder =
        DamlLf2.BuiltinType.getDescriptor.getValues.asScala
          .map(desc => DamlLf2.BuiltinType.internalGetValueMap().findValueByNumber(desc.getNumber))
          .sortBy(_.getNumber)
          .collect(protoMapping)

      primTypesInProtoOrder.sortBy(Ast.TBuiltin)(
        TypeOrdering.compare _
      ) shouldBe primTypesInProtoOrder
    }
  }

}
