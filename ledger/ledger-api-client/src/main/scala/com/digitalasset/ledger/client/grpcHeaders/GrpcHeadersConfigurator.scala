package com.digitalasset.ledger.client.grpcHeaders

import io.grpc.{Channel, ClientInterceptors}
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.AbstractStub

object GrpcHeadersConfigurator {

  def attachToStub[T <: AbstractStub[T]](
      stub: T,
      grpcHeaderDataInjector: Option[GrpcHeadersDataOrigin]): T = {
    grpcHeaderDataInjector.fold(stub)(inj =>
      stub.withInterceptors(new GrpcHeadersClientInterceptor(inj)))
  }

  def attachToChannelBuilder(
      channelBuilder: NettyChannelBuilder,
      grpcHeaderDataInjector: Option[GrpcHeadersDataOrigin]): NettyChannelBuilder = {
    grpcHeaderDataInjector.fold(channelBuilder)(inj =>
      channelBuilder.intercept(new GrpcHeadersClientInterceptor(inj)))
  }

  def attachToChannel(
      channel: Channel,
      grpcHeaderDataInjector: Option[GrpcHeadersDataOrigin]): Channel = {
    grpcHeaderDataInjector.fold(channel)(inj =>
      ClientInterceptors.intercept(channel, new GrpcHeadersClientInterceptor(inj)))
  }
}
