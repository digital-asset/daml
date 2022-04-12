// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import com.daml.lf.iface
import com.daml.lf.iface.{InterfaceType, TypeConName}
import com.daml.lf.data.Ref
import scalaz.std.list._

import scala.util.matching.Regex

object Util {

  private[codegen] def genTypeTopLevelDeclNames(genType: iface.Type): List[Ref.Identifier] =
    genType foldMapConsPrims {
      case TypeConName(nm) => List(nm)
      case _: com.daml.lf.iface.PrimType => Nil
    }

  // Template names can be filtered by given regexes (default: use all templates)
  // If a template does not match any regex, it becomes a "normal" datatype.
  private[codegen] def filterTemplatesBy(
      regexes: Seq[Regex]
  )(decls: Map[Ref.Identifier, InterfaceType]): Map[Ref.Identifier, InterfaceType] = {

    def matchesRoots(qualName: Ref.Identifier): Boolean =
      regexes.exists(_.findFirstIn(qualName.qualifiedName.qualifiedName).isDefined)

    if (regexes.isEmpty) decls
    else {
      decls transform {
        case (id, tpl @ InterfaceType.Template(_, _)) if !matchesRoots(id) =>
          InterfaceType.Normal(tpl.`type`)
        case (_, other) => other
      }
    }
  }

}
