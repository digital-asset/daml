// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import com.daml.lf.data.Ref
import com.daml.lf.typesig
import typesig.TypeConName
import typesig.PackageSignature.TypeDecl
import scalaz.std.list._

import scala.util.matching.Regex

object Util {

  private[codegen] def genTypeTopLevelDeclNames(genType: typesig.Type): List[Ref.Identifier] =
    genType foldMapConsPrims {
      case TypeConName(nm) => List(nm)
      case _: typesig.PrimType => Nil
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
