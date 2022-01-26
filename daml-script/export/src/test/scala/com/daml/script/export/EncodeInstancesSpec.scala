// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.{value => V}
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.language.Util._
import com.daml.script.export.Dependencies.{ChoiceInstanceSpec, TemplateInstanceSpec}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeInstancesSpec extends AnyFreeSpec with Matchers {
  import Encode._
  import AstSyntax._

  private val headerComment =
    """{- Module.Template is defined in a package using LF version 1.7 or earlier.
       |   These packages don't provide the required type class instances to generate
       |   ledger commands. The following defines replacement instances. -}""".stripMargin.replace(
      "\r\n",
      "\n",
    )

  "encodeMissingInstances" - {
    "template only" in {
      val tplId = ApiTypes.TemplateId(
        V.Identifier().withPackageId("pkg-id").withModuleName("Module").withEntityName("Template")
      )
      val spec = TemplateInstanceSpec(
        key = None,
        choices = Map.empty,
      )
      encodeMissingInstances(tplId, spec).render(80) shouldBe
        s"""$headerComment
           |instance HasTemplateTypeRep Module.Template where
           |  _templateTypeRep = GHC.Types.primitive @"ETemplateTypeRep"
           |instance HasToAnyTemplate Module.Template where
           |  _toAnyTemplate = GHC.Types.primitive @"EToAnyTemplate"""".stripMargin.replace(
          "\r\n",
          "\n",
        )
    }
    "template with choices" in {
      val tplId = ApiTypes.TemplateId(
        V.Identifier()
          .withPackageId("e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b")
          .withModuleName("Module")
          .withEntityName("Template")
      )
      val choiceArg = Ast.TTyCon(
        Ref.TypeConName.assertFromString(
          "e7b2c7155f6dd6fc569c2325be821f1269186a540d0408b9a0c9e30406f6b64b:Module:Choice"
        )
      )
      val spec = TemplateInstanceSpec(
        key = None,
        choices = Map(
          ApiTypes.Choice("Archive") -> ChoiceInstanceSpec(tArchive, TUnit),
          ApiTypes.Choice("Choice") -> ChoiceInstanceSpec(choiceArg, TContractId(Ast.TTyCon(nFoo))),
        ),
      )
      encodeMissingInstances(tplId, spec).render(80) shouldBe
        s"""$headerComment
           |instance HasTemplateTypeRep Module.Template where
           |  _templateTypeRep = GHC.Types.primitive @"ETemplateTypeRep"
           |instance HasToAnyTemplate Module.Template where
           |  _toAnyTemplate = GHC.Types.primitive @"EToAnyTemplate"
           |instance HasToAnyChoice Module.Template DA.Internal.Template.Archive () where
           |  _toAnyChoice = GHC.Types.primitive @"EToAnyChoice"
           |instance HasToAnyChoice Module.Template Module.Choice (ContractId Module.Foo) where
           |  _toAnyChoice = GHC.Types.primitive @"EToAnyChoice"""".stripMargin
          .replace("\r\n", "\n")
    }
    "template with contract key" in {
      val tplId = ApiTypes.TemplateId(
        V.Identifier().withPackageId("pkg-id").withModuleName("Module").withEntityName("Template")
      )
      val spec = TemplateInstanceSpec(
        key = Some(tuple(TParty, TInt64)),
        choices = Map.empty,
      )
      encodeMissingInstances(tplId, spec).render(80) shouldBe
        s"""$headerComment
           |instance HasTemplateTypeRep Module.Template where
           |  _templateTypeRep = GHC.Types.primitive @"ETemplateTypeRep"
           |instance HasToAnyTemplate Module.Template where
           |  _toAnyTemplate = GHC.Types.primitive @"EToAnyTemplate"
           |instance HasToAnyContractKey Module.Template (Party, Int) where
           |  _toAnyContractKey = GHC.Types.primitive @"EToAnyContractKey"""".stripMargin.replace(
          "\r\n",
          "\n",
        )
    }
  }
}
