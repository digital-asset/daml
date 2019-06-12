// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml_lf.DamlLf1
import org.scalatest.{Matchers, WordSpec}

class DecodeV1Spec extends WordSpec with Matchers {

  "The keys of primTypeTable correspond to Protobuf DamlLf1.PrimType" in {

    (DecodeV1.primTypeTable.keySet + DamlLf1.PrimType.UNRECOGNIZED) shouldBe
      DamlLf1.PrimType.values().toSet

  }

  "The keys of builtinFunctionMap correspond to Protobuf DamlLf1.BuiltinFunction" in {

    (DecodeV1.builtinFunctionMap.keySet + DamlLf1.BuiltinFunction.UNRECOGNIZED) shouldBe
      DamlLf1.BuiltinFunction.values().toSet

  }

}
