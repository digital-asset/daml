// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import com.google.protobuf.any.{Any => AnyProto}
import com.google.protobuf.{Any => AnyJavaProto}
import com.google.rpc.status.{Status => StatusProto}
import com.google.rpc.{ErrorInfo, Status => StatusJavaProto}
import io.grpc.Status.Code
import io.grpc.{Metadata, Status}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object GrpcStatus {
  type Description = Option[String]
  type CodeValue = Int

  def unapply(status: Status): Some[(Code, Description)] =
    Some((status.getCode, Option(status.getDescription)))

  def buildStatus(
      metadata: Map[String, String],
      status: StatusJavaProto.Builder,
  ): StatusJavaProto = {
    import scala.jdk.CollectionConverters._
    val details = mergeDetails(metadata, status)
    status
      .clearDetails()
      .addAllDetails(details.asJava)
      .build()
  }

  /** As [[io.grpc.Status]] and [[com.google.rpc.status.Status]] aren't isomorphic i.e. the former one
    * doesn't contain details, this function takes an additional `metadata` argument to restore them.
    */
  def toProto(status: Status, metadata: Metadata): StatusProto = {
    val code = status.getCode.value
    val description = Option(status.getDescription).getOrElse("")
    val statusJavaProto = io.grpc.protobuf.StatusProto.fromStatusAndTrailers(status, metadata)
    StatusProto(code, description, extractDetails(statusJavaProto))
  }

  def toJavaProto(status: StatusProto): StatusJavaProto = toJavaBuilder(status).build()

  def toJavaBuilder(status: StatusProto): StatusJavaProto.Builder =
    toJavaBuilder(
      status.code,
      Some(status.message),
      status.details.map(AnyProto.toJavaProto),
    )

  def toJavaBuilder(status: Status): StatusJavaProto.Builder =
    toJavaBuilder(status.getCode.value(), Option(status.getDescription), Iterable.empty)

  def toJavaBuilder(
      code: CodeValue,
      description: Description,
      details: Iterable[AnyJavaProto],
  ): StatusJavaProto.Builder =
    StatusJavaProto
      .newBuilder()
      .setCode(code)
      .setMessage(description.getOrElse(""))
      .addAllDetails(details.asJava)

  private[grpc] final class SpecificGrpcStatus(code: Code) {
    def unapply(status: Status): Boolean =
      status.getCode == code
  }

  private def extractDetails(statusJavaProto: StatusJavaProto): Seq[AnyProto] =
    statusJavaProto.getDetailsList.asScala.map(detail => AnyProto.fromJavaProto(detail)).toSeq

  private def mergeDetails(
      metadata: Map[String, String],
      status: StatusJavaProto.Builder,
  ): mutable.Seq[AnyJavaProto] = {
    import scala.jdk.CollectionConverters._
    val previousDetails = status.getDetailsList.asScala
    val newDetails = if (previousDetails.exists(_.is(classOf[ErrorInfo]))) {
      previousDetails.map {
        case detail if detail.is(classOf[ErrorInfo]) =>
          val previousErrorInfo: ErrorInfo = detail.unpack(classOf[ErrorInfo])
          val newErrorInfo = previousErrorInfo.toBuilder.putAllMetadata(metadata.asJava).build()
          AnyJavaProto.pack(newErrorInfo)
        case otherDetail => otherDetail
      }
    } else {
      previousDetails :+ AnyJavaProto.pack(
        ErrorInfo.newBuilder().putAllMetadata(metadata.asJava).build()
      )
    }
    newDetails
  }

  val OK = new SpecificGrpcStatus(Code.OK)
  val CANCELLED = new SpecificGrpcStatus(Code.CANCELLED)
  val UNKNOWN = new SpecificGrpcStatus(Code.UNKNOWN)
  val INVALID_ARGUMENT = new SpecificGrpcStatus(Code.INVALID_ARGUMENT)
  val DEADLINE_EXCEEDED = new SpecificGrpcStatus(Code.DEADLINE_EXCEEDED)
  val NOT_FOUND = new SpecificGrpcStatus(Code.NOT_FOUND)
  val ALREADY_EXISTS = new SpecificGrpcStatus(Code.ALREADY_EXISTS)
  val PERMISSION_DENIED = new SpecificGrpcStatus(Code.PERMISSION_DENIED)
  val RESOURCE_EXHAUSTED = new SpecificGrpcStatus(Code.RESOURCE_EXHAUSTED)
  val FAILED_PRECONDITION = new SpecificGrpcStatus(Code.FAILED_PRECONDITION)
  val ABORTED = new SpecificGrpcStatus(Code.ABORTED)
  val OUT_OF_RANGE = new SpecificGrpcStatus(Code.OUT_OF_RANGE)
  val UNIMPLEMENTED = new SpecificGrpcStatus(Code.UNIMPLEMENTED)
  val INTERNAL = new SpecificGrpcStatus(Code.INTERNAL)
  val UNAVAILABLE = new SpecificGrpcStatus(Code.UNAVAILABLE)
  val DATA_LOSS = new SpecificGrpcStatus(Code.DATA_LOSS)
  val UNAUTHENTICATED = new SpecificGrpcStatus(Code.UNAUTHENTICATED)
}
