// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.ledger.api.v1.TransactionServiceOuterClass
import com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
import io.grpc.MethodDescriptor

object TransactionServiceGrpc {
  val METHOD_GET_TRANSACTIONS: MethodDescriptor[
    GetTransactionsRequest,
    TransactionServiceOuterClass.GetTransactionsResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor
          .generateFullMethodName("com.daml.ledger.api.v1.TransactionService", "GetTransactions")
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest]
      )
      .setResponseMarshaller(
        io.grpc.protobuf.ProtoUtils
          .marshaller(TransactionServiceOuterClass.GetTransactionsResponse.getDefaultInstance)
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(0)
        )
      )
      .build()

  val METHOD_GET_TRANSACTION_TREES: MethodDescriptor[
    GetTransactionsRequest,
    TransactionServiceOuterClass.GetTransactionTreesResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor.generateFullMethodName(
          "com.daml.ledger.api.v1.TransactionService",
          "GetTransactionTrees",
        )
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest]
      )
      .setResponseMarshaller(
        io.grpc.protobuf.ProtoUtils
          .marshaller(TransactionServiceOuterClass.GetTransactionTreesResponse.getDefaultInstance)
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(1)
        )
      )
      .build()

  val METHOD_GET_TRANSACTION_BY_EVENT_ID: _root_.io.grpc.MethodDescriptor[
    com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest,
    com.daml.ledger.api.v1.transaction_service.GetTransactionResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor.generateFullMethodName(
          "com.daml.ledger.api.v1.TransactionService",
          "GetTransactionByEventId",
        )
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest]
      )
      .setResponseMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionResponse]
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(2)
        )
      )
      .build()

  val METHOD_GET_TRANSACTION_BY_ID: _root_.io.grpc.MethodDescriptor[
    com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest,
    com.daml.ledger.api.v1.transaction_service.GetTransactionResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor
          .generateFullMethodName("com.daml.ledger.api.v1.TransactionService", "GetTransactionById")
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest]
      )
      .setResponseMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionResponse]
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(3)
        )
      )
      .build()

  val METHOD_GET_FLAT_TRANSACTION_BY_EVENT_ID: _root_.io.grpc.MethodDescriptor[
    com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest,
    com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor.generateFullMethodName(
          "com.daml.ledger.api.v1.TransactionService",
          "GetFlatTransactionByEventId",
        )
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest]
      )
      .setResponseMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse]
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(4)
        )
      )
      .build()

  val METHOD_GET_FLAT_TRANSACTION_BY_ID: _root_.io.grpc.MethodDescriptor[
    com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest,
    com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor.generateFullMethodName(
          "com.daml.ledger.api.v1.TransactionService",
          "GetFlatTransactionById",
        )
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest]
      )
      .setResponseMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse]
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(5)
        )
      )
      .build()

  val METHOD_GET_LEDGER_END: _root_.io.grpc.MethodDescriptor[
    com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest,
    com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse,
  ] =
    _root_.io.grpc.MethodDescriptor
      .newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(
        _root_.io.grpc.MethodDescriptor
          .generateFullMethodName("com.daml.ledger.api.v1.TransactionService", "GetLedgerEnd")
      )
      .setSampledToLocalTracing(true)
      .setRequestMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest]
      )
      .setResponseMarshaller(
        _root_.scalapb.grpc.Marshaller
          .forMessage[com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse]
      )
      .setSchemaDescriptor(
        _root_.scalapb.grpc.ConcreteProtoMethodDescriptorSupplier.fromMethodDescriptor(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
            .getServices()
            .get(0)
            .getMethods()
            .get(6)
        )
      )
      .build()

  val SERVICE: _root_.io.grpc.ServiceDescriptor =
    _root_.io.grpc.ServiceDescriptor
      .newBuilder("com.daml.ledger.api.v1.TransactionService")
      .setSchemaDescriptor(
        new _root_.scalapb.grpc.ConcreteProtoFileDescriptorSupplier(
          com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
        )
      )
      .addMethod(METHOD_GET_TRANSACTIONS)
      .addMethod(METHOD_GET_TRANSACTION_TREES)
      .addMethod(METHOD_GET_TRANSACTION_BY_EVENT_ID)
      .addMethod(METHOD_GET_TRANSACTION_BY_ID)
      .addMethod(METHOD_GET_FLAT_TRANSACTION_BY_EVENT_ID)
      .addMethod(METHOD_GET_FLAT_TRANSACTION_BY_ID)
      .addMethod(METHOD_GET_LEDGER_END)
      .build()

  /** Allows clients to read transactions from the ledger.
    */
  trait TransactionService extends _root_.scalapb.grpc.AbstractService {
    override def serviceCompanion = TransactionService

    /** Read the ledger's filtered transaction stream for a set of parties.
      * Lists only creates and archives, but not other events.
      * Omits all events on transient contracts, i.e., contracts that were both created and archived in the same transaction.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    def getTransactions(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
        responseObserver: _root_.io.grpc.stub.StreamObserver[
          TransactionServiceOuterClass.GetTransactionsResponse
        ],
    ): _root_.scala.Unit

    /** Read the ledger's complete transaction tree stream for a set of parties.
      * The stream can be filtered only by parties, but not templates (template filter must be empty).
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    def getTransactionTrees(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
        responseObserver: _root_.io.grpc.stub.StreamObserver[
          TransactionServiceOuterClass.GetTransactionTreesResponse
        ],
    ): _root_.scala.Unit

    /** Lookup a transaction tree by the ID of an event that appears within it.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionByEventId
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): scala.concurrent.Future[com.daml.ledger.api.v1.transaction_service.GetTransactionResponse]

    /** Lookup a transaction tree by its ID.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionById
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): scala.concurrent.Future[com.daml.ledger.api.v1.transaction_service.GetTransactionResponse]

    /** Lookup a transaction by the ID of an event that appears within it.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getFlatTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): scala.concurrent.Future[
      com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse
    ]

    /** Lookup a transaction by its ID.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getFlatTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): scala.concurrent.Future[
      com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse
    ]

    /** Get the current ledger end.
      * Subscriptions started with the returned offset will serve transactions created after this RPC was called.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      */
    def getLedgerEnd(
        request: com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest
    ): scala.concurrent.Future[com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse]
  }

  object TransactionService extends _root_.scalapb.grpc.ServiceCompanion[TransactionService] {
    implicit def serviceCompanion: _root_.scalapb.grpc.ServiceCompanion[TransactionService] = this
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor =
      com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
        .getServices()
        .get(0)
    def scalaDescriptor: _root_.scalapb.descriptors.ServiceDescriptor =
      com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.scalaDescriptor.services(0)
    def bindService(
        serviceImpl: TransactionService,
        executionContext: scala.concurrent.ExecutionContext,
    ): _root_.io.grpc.ServerServiceDefinition =
      _root_.io.grpc.ServerServiceDefinition
        .builder(SERVICE)
        .addMethod(
          METHOD_GET_TRANSACTIONS,
          _root_.io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new _root_.io.grpc.stub.ServerCalls.ServerStreamingMethod[
              com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
              TransactionServiceOuterClass.GetTransactionsResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    TransactionServiceOuterClass.GetTransactionsResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl.getTransactions(request, observer)
            }
          ),
        )
        .addMethod(
          METHOD_GET_TRANSACTION_TREES,
          _root_.io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new _root_.io.grpc.stub.ServerCalls.ServerStreamingMethod[
              com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
              TransactionServiceOuterClass.GetTransactionTreesResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    TransactionServiceOuterClass.GetTransactionTreesResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl.getTransactionTrees(request, observer)
            }
          ),
        )
        .addMethod(
          METHOD_GET_TRANSACTION_BY_EVENT_ID,
          _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(
            new _root_.io.grpc.stub.ServerCalls.UnaryMethod[
              com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest,
              com.daml.ledger.api.v1.transaction_service.GetTransactionResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    com.daml.ledger.api.v1.transaction_service.GetTransactionResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl
                  .getTransactionByEventId(request)
                  .onComplete(scalapb.grpc.Grpc.completeObserver(observer))(executionContext)
            }
          ),
        )
        .addMethod(
          METHOD_GET_TRANSACTION_BY_ID,
          _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(
            new _root_.io.grpc.stub.ServerCalls.UnaryMethod[
              com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest,
              com.daml.ledger.api.v1.transaction_service.GetTransactionResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    com.daml.ledger.api.v1.transaction_service.GetTransactionResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl
                  .getTransactionById(request)
                  .onComplete(scalapb.grpc.Grpc.completeObserver(observer))(executionContext)
            }
          ),
        )
        .addMethod(
          METHOD_GET_FLAT_TRANSACTION_BY_EVENT_ID,
          _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(
            new _root_.io.grpc.stub.ServerCalls.UnaryMethod[
              com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest,
              com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl
                  .getFlatTransactionByEventId(request)
                  .onComplete(scalapb.grpc.Grpc.completeObserver(observer))(executionContext)
            }
          ),
        )
        .addMethod(
          METHOD_GET_FLAT_TRANSACTION_BY_ID,
          _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(
            new _root_.io.grpc.stub.ServerCalls.UnaryMethod[
              com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest,
              com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl
                  .getFlatTransactionById(request)
                  .onComplete(scalapb.grpc.Grpc.completeObserver(observer))(executionContext)
            }
          ),
        )
        .addMethod(
          METHOD_GET_LEDGER_END,
          _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(
            new _root_.io.grpc.stub.ServerCalls.UnaryMethod[
              com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest,
              com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse,
            ] {
              override def invoke(
                  request: com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest,
                  observer: _root_.io.grpc.stub.StreamObserver[
                    com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse
                  ],
              ): _root_.scala.Unit =
                serviceImpl
                  .getLedgerEnd(request)
                  .onComplete(scalapb.grpc.Grpc.completeObserver(observer))(executionContext)
            }
          ),
        )
        .build()
  }

  /** Allows clients to read transactions from the ledger.
    */
  trait TransactionServiceBlockingClient {
    def serviceCompanion = TransactionService

    /** Read the ledger's filtered transaction stream for a set of parties.
      * Lists only creates and archives, but not other events.
      * Omits all events on transient contracts, i.e., contracts that were both created and archived in the same transaction.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    def getTransactions(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
    ): Iterator[TransactionServiceOuterClass.GetTransactionsResponse]

    /** Read the ledger's complete transaction tree stream for a set of parties.
      * The stream can be filtered only by parties, but not templates (template filter must be empty).
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    def getTransactionTrees(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
    ): scala.collection.Iterator[
      TransactionServiceOuterClass.GetTransactionTreesResponse
    ]

    /** Lookup a transaction tree by the ID of an event that appears within it.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionByEventId
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetTransactionResponse

    /** Lookup a transaction tree by its ID.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionById
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetTransactionResponse

    /** Lookup a transaction by the ID of an event that appears within it.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getFlatTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse

    /** Lookup a transaction by its ID.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    def getFlatTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse

    /** Get the current ledger end.
      * Subscriptions started with the returned offset will serve transactions created after this RPC was called.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      */
    def getLedgerEnd(
        request: com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest
    ): com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse
  }

  class TransactionServiceBlockingStub(
      channel: _root_.io.grpc.Channel,
      options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT,
  ) extends _root_.io.grpc.stub.AbstractStub[TransactionServiceBlockingStub](channel, options)
      with TransactionServiceBlockingClient {

    /** Read the ledger's filtered transaction stream for a set of parties.
      * Lists only creates and archives, but not other events.
      * Omits all events on transient contracts, i.e., contracts that were both created and archived in the same transaction.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    override def getTransactions(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
    ): Iterator[TransactionServiceOuterClass.GetTransactionsResponse] = {
      _root_.scalapb.grpc.ClientCalls.blockingServerStreamingCall(
        channel,
        METHOD_GET_TRANSACTIONS,
        options,
        request,
      )
    }

    /** Read the ledger's complete transaction tree stream for a set of parties.
      * The stream can be filtered only by parties, but not templates (template filter must be empty).
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    override def getTransactionTrees(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
    ): scala.collection.Iterator[
      TransactionServiceOuterClass.GetTransactionTreesResponse
    ] = {
      _root_.scalapb.grpc.ClientCalls.blockingServerStreamingCall(
        channel,
        METHOD_GET_TRANSACTION_TREES,
        options,
        request,
      )
    }

    /** Lookup a transaction tree by the ID of an event that appears within it.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionByEventId
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetTransactionResponse = {
      _root_.scalapb.grpc.ClientCalls.blockingUnaryCall(
        channel,
        METHOD_GET_TRANSACTION_BY_EVENT_ID,
        options,
        request,
      )
    }

    /** Lookup a transaction tree by its ID.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionById
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetTransactionResponse = {
      _root_.scalapb.grpc.ClientCalls.blockingUnaryCall(
        channel,
        METHOD_GET_TRANSACTION_BY_ID,
        options,
        request,
      )
    }

    /** Lookup a transaction by the ID of an event that appears within it.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getFlatTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse = {
      _root_.scalapb.grpc.ClientCalls.blockingUnaryCall(
        channel,
        METHOD_GET_FLAT_TRANSACTION_BY_EVENT_ID,
        options,
        request,
      )
    }

    /** Lookup a transaction by its ID.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getFlatTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse = {
      _root_.scalapb.grpc.ClientCalls.blockingUnaryCall(
        channel,
        METHOD_GET_FLAT_TRANSACTION_BY_ID,
        options,
        request,
      )
    }

    /** Get the current ledger end.
      * Subscriptions started with the returned offset will serve transactions created after this RPC was called.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      */
    override def getLedgerEnd(
        request: com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest
    ): com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse = {
      _root_.scalapb.grpc.ClientCalls.blockingUnaryCall(
        channel,
        METHOD_GET_LEDGER_END,
        options,
        request,
      )
    }

    override def build(
        channel: _root_.io.grpc.Channel,
        options: _root_.io.grpc.CallOptions,
    ): TransactionServiceBlockingStub = new TransactionServiceBlockingStub(channel, options)
  }

  class TransactionServiceStub(
      channel: _root_.io.grpc.Channel,
      options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT,
  ) extends _root_.io.grpc.stub.AbstractStub[TransactionServiceStub](channel, options)
      with TransactionService {

    /** Read the ledger's filtered transaction stream for a set of parties.
      * Lists only creates and archives, but not other events.
      * Omits all events on transient contracts, i.e., contracts that were both created and archived in the same transaction.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    override def getTransactions(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
        responseObserver: _root_.io.grpc.stub.StreamObserver[
          TransactionServiceOuterClass.GetTransactionsResponse
        ],
    ): _root_.scala.Unit = {
      _root_.scalapb.grpc.ClientCalls.asyncServerStreamingCall(
        channel,
        METHOD_GET_TRANSACTIONS,
        options,
        request,
        responseObserver,
      )
    }

    /** Read the ledger's complete transaction tree stream for a set of parties.
      * The stream can be filtered only by parties, but not templates (template filter must be empty).
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if ``before`` is not before ``end``)
      * - ``FAILED_PRECONDITION``: if the ledger has been pruned after the subscription start offset
      * - ``OUT_OF_RANGE``: if the ``begin`` parameter value is not before the end of the ledger
      */
    override def getTransactionTrees(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
        responseObserver: _root_.io.grpc.stub.StreamObserver[
          TransactionServiceOuterClass.GetTransactionTreesResponse
        ],
    ): _root_.scala.Unit = {
      _root_.scalapb.grpc.ClientCalls.asyncServerStreamingCall(
        channel,
        METHOD_GET_TRANSACTION_TREES,
        options,
        request,
        responseObserver,
      )
    }

    /** Lookup a transaction tree by the ID of an event that appears within it.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionByEventId
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): scala.concurrent.Future[
      com.daml.ledger.api.v1.transaction_service.GetTransactionResponse
    ] = {
      _root_.scalapb.grpc.ClientCalls.asyncUnaryCall(
        channel,
        METHOD_GET_TRANSACTION_BY_EVENT_ID,
        options,
        request,
      )
    }

    /** Lookup a transaction tree by its ID.
      * For looking up a transaction instead of a transaction tree, please see GetFlatTransactionById
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): scala.concurrent.Future[
      com.daml.ledger.api.v1.transaction_service.GetTransactionResponse
    ] = {
      _root_.scalapb.grpc.ClientCalls.asyncUnaryCall(
        channel,
        METHOD_GET_TRANSACTION_BY_ID,
        options,
        request,
      )
    }

    /** Lookup a transaction by the ID of an event that appears within it.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getFlatTransactionByEventId(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByEventIdRequest
    ): scala.concurrent.Future[
      com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse
    ] = {
      _root_.scalapb.grpc.ClientCalls.asyncUnaryCall(
        channel,
        METHOD_GET_FLAT_TRANSACTION_BY_EVENT_ID,
        options,
        request,
      )
    }

    /** Lookup a transaction by its ID.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id or no such transaction exists
      * - ``INVALID_ARGUMENT``: if the payload is malformed or is missing required fields (e.g. if requesting parties are invalid or empty)
      */
    override def getFlatTransactionById(
        request: com.daml.ledger.api.v1.transaction_service.GetTransactionByIdRequest
    ): scala.concurrent.Future[
      com.daml.ledger.api.v1.transaction_service.GetFlatTransactionResponse
    ] = {
      _root_.scalapb.grpc.ClientCalls.asyncUnaryCall(
        channel,
        METHOD_GET_FLAT_TRANSACTION_BY_ID,
        options,
        request,
      )
    }

    /** Get the current ledger end.
      * Subscriptions started with the returned offset will serve transactions created after this RPC was called.
      * Errors:
      * - ``UNAUTHENTICATED``: if the request does not include a valid access token
      * - ``PERMISSION_DENIED``: if the claims in the token are insufficient to perform a given operation
      * - ``NOT_FOUND``: if the request does not include a valid ledger id
      */
    override def getLedgerEnd(
        request: com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest
    ): scala.concurrent.Future[com.daml.ledger.api.v1.transaction_service.GetLedgerEndResponse] = {
      _root_.scalapb.grpc.ClientCalls.asyncUnaryCall(
        channel,
        METHOD_GET_LEDGER_END,
        options,
        request,
      )
    }

    override def build(
        channel: _root_.io.grpc.Channel,
        options: _root_.io.grpc.CallOptions,
    ): TransactionServiceStub = new TransactionServiceStub(channel, options)
  }

  def bindService(
      serviceImpl: TransactionService,
      executionContext: scala.concurrent.ExecutionContext,
  ): _root_.io.grpc.ServerServiceDefinition =
    TransactionService.bindService(serviceImpl, executionContext)

  def blockingStub(channel: _root_.io.grpc.Channel): TransactionServiceBlockingStub =
    new TransactionServiceBlockingStub(channel)

  def stub(channel: _root_.io.grpc.Channel): TransactionServiceStub = new TransactionServiceStub(
    channel
  )

  def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor =
    com.daml.ledger.api.v1.transaction_service.TransactionServiceProto.javaDescriptor
      .getServices()
      .get(0)

}
