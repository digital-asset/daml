// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.daml.lf.codegen.backend.Backend
import com.daml.lf.codegen.backend.java.inner.{ClassForType, DecoderClass}
import com.daml.lf.codegen.conf.Conf
import com.daml.lf.codegen.{InterfaceTrees, ModuleWithContext, NodeWithContext}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.Interface
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import org.slf4j.MDC

import scala.concurrent.{ExecutionContext, Future}

private[codegen] object JavaBackend extends Backend with StrictLogging {

  override def preprocess(
      interfaces: Seq[Interface],
      conf: Conf,
      packagePrefixes: Map[PackageId, String],
  )(implicit ec: ExecutionContext): Future[InterfaceTrees] = {
    val tree = InterfaceTrees.fromInterfaces(interfaces)
    for ((decoderPkg, decoderClassName) <- conf.decoderPkgAndClass) {
      val templateNames = extractTemplateNames(tree, packagePrefixes)
      val decoderFile = JavaFile
        .builder(
          decoderPkg,
          DecoderClass.generateCode(decoderClassName, templateNames),
        )
        .build()
      decoderFile.writeTo(conf.outputDirectory)
    }
    Future.successful(tree)
  }

  private def extractTemplateNames(
      tree: InterfaceTrees,
      packagePrefixes: Map[PackageId, String],
  ) = {
    tree.interfaceTrees.flatMap(_.bfs(Vector[ClassName]()) {
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
      conf: Conf,
      packagePrefixes: Map[PackageId, String],
  )(implicit ec: ExecutionContext): Future[Unit] = {
    nodeWithContext match {
      case moduleWithContext: ModuleWithContext if moduleWithContext.module.types.nonEmpty =>
        // this is a Daml module that contains type declarations => the codegen will create one file
        Future {
          logger.info(
            s"Generating code for module ${moduleWithContext.lineage.map(_._1).toSeq.mkString(".")}"
          )
          for (javaFile <- createTypeDefinitionClasses(moduleWithContext, packagePrefixes)) {
            logger.info(
              s"Writing ${javaFile.packageName}.${javaFile.typeSpec.name} to directory ${conf.outputDirectory}"
            )
            javaFile.writeTo(conf.outputDirectory)

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
    val typeSpecs = for {
      typeWithContext <- moduleWithContext.typesLineages
      javaFile <- ClassForType(typeWithContext, packagePrefixes)
    } yield {
      javaFile
    }
    MDC.remove("packageId")
    MDC.remove("packageIdShort")
    MDC.remove("moduleName")
    typeSpecs
  }
}
