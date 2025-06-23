// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.typesig._
import com.digitalasset.daml.lf.typesig.PackageSignature.TypeDecl
import scala.util.matching.Regex

object Util {

  private[codegen] def genTypeTopLevelDeclNames(genType: Type): List[Ref.Identifier] =
    genType match {
      case TypeCon(TypeConId(nm), typArgs) => nm :: typArgs.toList.flatMap(genTypeTopLevelDeclNames)
      case TypePrim(_, typArgs) => typArgs.toList.flatMap(genTypeTopLevelDeclNames)
      case TypeVar(_) | TypeNumeric(_) => Nil
    }

  // Template names can be filtered by given regexes (default: use all templates)
  // If a template does not match any regex, it becomes a "normal" datatype.
  private[codegen] def filterTemplatesBy(
      regexes: Seq[Regex]
  )(decls: Map[Ref.Identifier, TypeDecl]): Map[Ref.Identifier, TypeDecl] = {

    def matchesRoots(qualName: Ref.Identifier): Boolean =
      regexes.exists(_.findFirstIn(qualName.qualifiedName.qualifiedName).isDefined)

    if (regexes.isEmpty) decls
    else {
      decls transform {
        case (id, tpl @ TypeDecl.Template(_, _)) if !matchesRoots(id) =>
          TypeDecl.Normal(tpl.`type`)
        case (_, other) => other
      }
    }
  }

}
