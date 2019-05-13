// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.data.{BackStack, BackStackSnoc, FrontStack}
import com.digitalasset.daml.lf.data.Ref.{DottedName, Name, ModuleName, QualifiedName}
import com.digitalasset.daml.lf.lfpackage.Ast.Package

import scala.annotation.tailrec

/** Class useful to convert old-style dotted identifiers into new-style identifiers. */
object DeprecatedIdentifier {
  def lookup(pkg: Package, deprecatedIdentifier: String): Either[String, QualifiedName] = {
    // in the function below we use unsafeFromSegments since we're looking up in
    // the package anyway.

    val nameParts = deprecatedIdentifier.split("\\.").map(Name.assertFromString)

    if (nameParts.isEmpty) {
      Left(
        s"Illegal input given to lookup template: $deprecatedIdentifier, expected qualified name")
    } else {
      nameParts.lastOption match {
        case None =>
          Left(s"Illegal input to lookup template -- empty identifier")
        case Some(name) =>
          @tailrec
          def go(
              modulePart: BackStack[Name],
              namePart: FrontStack[Name]
          ): Either[String, QualifiedName] = {
            val moduleId = modulePart.toImmArray
            val name = DottedName.unsafeFromNames(namePart.toImmArray)
            pkg.modules.get(ModuleName.unsafeFromNames(moduleId)) match {
              case Some(module) =>
                module.definitions.get(name) match {
                  case Some(_) =>
                    Right(QualifiedName(ModuleName.unsafeFromNames(moduleId), name))
                  case None =>
                    modulePart match {
                      case BackStack() =>
                        Left(s"Could not find definition $name")
                      case BackStackSnoc(modulePart_, segment) =>
                        go(modulePart_, segment +: namePart)
                    }
                }
              case None =>
                modulePart match {
                  case BackStack() => Left(s"Could not find definition $name")
                  case BackStackSnoc(modulePart_, segment) =>
                    go(modulePart_, segment +: namePart)
                }
            }
          }
          go(BackStack(nameParts.toList.dropRight(1)), FrontStack(name))
      }
    }
  }

  /** Renders a qualified name in the old deprecated style -- with a dot between
    * module and name.
    */
  def toString(qualifiedName: QualifiedName): String =
    qualifiedName.module.toString + "." + qualifiedName.name.toString
}
