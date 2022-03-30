// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import java.nio.file.Path

import com.daml.lf.codegen.backend.Backend
import com.daml.lf.codegen.backend.java.inner.{ClassForType, InterfaceClass, DecoderClass}
import com.daml.lf.codegen.{NodeWithContext, ModuleWithContext, InterfaceTree}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.Interface
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.MDC

import scala.concurrent.{Future, ExecutionContext}

private[codegen] object JavaBackend extends Backend with StrictLogging {

  override def preprocess(
      interfaces: Seq[Interface],
      outputDirectory: Path,
      decoderPkgAndClass: Option[(String, String)] = None,
      packagePrefixes: Map[PackageId, String],
  )(implicit ec: ExecutionContext): Future[Seq[InterfaceTree]] = {
    val tree = interfaces.map(InterfaceTree.fromInterface)
    for ((decoderPkg, decoderClassName) <- decoderPkgAndClass) {
      val templateNames = extractTemplateNames(tree, packagePrefixes)
      val decoderFile = JavaFile
        .builder(
          decoderPkg,
          DecoderClass.generateCode(decoderClassName, templateNames),
        )
        .build()
      decoderFile.writeTo(outputDirectory)
    }
    Future.successful(tree)
  }

  private def extractTemplateNames(
      interfaceTrees: Seq[InterfaceTree],
      packagePrefixes: Map[PackageId, String],
  ) = {
    interfaceTrees.flatMap(_.bfs(Vector[ClassName]()) {
      case (res, module: ModuleWithContext) =>
        val templateNames = module.typesLineages
          .collect {
            case t if t.`type`.typ.exists(_.getTemplate.isPresent) =>
              ClassName.bestGuess(inner.fullyQualifiedName(t.identifier, packagePrefixes))
          }
        res ++ templateNames
      case (res, _) => res
    })
  }

  def process(
      nodeWithContext: NodeWithContext,
      packagePrefixes: Map[PackageId, String],
      outputDirectory: Path,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    nodeWithContext match {
      case moduleWithContext: ModuleWithContext =>
        // this is a Daml module that contains type declarations => the codegen will create one file
        Future {
          logger.info(
            s"Generating code for module ${moduleWithContext.lineage.map(_._1).toSeq.mkString(".")}"
          )
          for (javaFile <- createTypeDefinitionClasses(moduleWithContext, packagePrefixes)) {
            logger.info(
              s"Writing ${javaFile.packageName}.${javaFile.typeSpec.name} to directory $outputDirectory"
            )
            javaFile.writeTo(outputDirectory)
          }
        }
      case _ =>
        Future.unit
    }
  }

  private def createTypeDefinitionClasses(
      moduleWithContext: ModuleWithContext,
      packagePrefixes: Map[PackageId, String],
  ): Iterable[JavaFile] = {
    MDC.put("packageId", moduleWithContext.packageId)
    MDC.put("packageIdShort", moduleWithContext.packageId.take(7))
    MDC.put("moduleName", moduleWithContext.name)
    val typeSpecs = {
      moduleWithContext.typesLineages.flatMap { typeWithContext =>
        typeWithContext.interface.astInterfaces.map { case (interfaceName, interface) =>
          val className = InterfaceClass.classNameForInterface(interfaceName)
          val javaPackage = className.packageName()
          JavaFile
            .builder(
              javaPackage,
              InterfaceClass
                .generate(
                  className,
                  interface,
                  packagePrefixes,
                  moduleWithContext.packageId,
                  interfaceName,
                ),
            )
            .build()
        } ++ ClassForType(typeWithContext, packagePrefixes)
      }
    }
    MDC.remove("packageId")
    MDC.remove("packageIdShort")
    MDC.remove("moduleName")
    typeSpecs
  }
}
