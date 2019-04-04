-- | Low-level safe interface to gRPC. By "safe", we mean:
-- 1. all gRPC objects are guaranteed to be cleaned up correctly.
-- 2. all functions are thread-safe.
-- 3. all functions leave gRPC in a consistent, safe state.
-- These guarantees only apply to the functions exported by this module,
-- and not to helper functions in submodules that aren't exported here.

{-# LANGUAGE RecordWildCards #-}

module Network.GRPC.LowLevel (
-- * Important types
GRPC
, withGRPC
, GRPCIOError(..)
, StatusCode(..)

-- * Completion queue utilities
, CompletionQueue
, withCompletionQueue

-- * Calls
, GRPCMethodType(..)
, RegisteredMethod
, MethodPayload
, NormalRequestResult(..)
, MetadataMap(..)
, MethodName(..)
, StatusDetails(..)

-- * Configuration options
, Arg(..)
, CompressionAlgorithm(..)
, CompressionLevel(..)
, Host(..)
, Port(..)

-- * Server
, ServerConfig(..)
, Server(normalMethods, sstreamingMethods, cstreamingMethods,
         bidiStreamingMethods)
, ServerCall(payload, metadata)
, withServer
, serverHandleNormalCall
, ServerHandlerLL
, withServerCall
, serverCallCancel
, serverCallIsExpired
, serverReader -- for client streaming
, ServerReaderHandlerLL
, serverWriter -- for server streaming
, ServerWriterHandlerLL
, serverRW     -- for bidirectional streaming
, ServerRWHandlerLL

-- * Client and Server Auth
, AuthContext
, AuthProperty(..)
, getAuthProperties
, addAuthProperty

-- * Server Auth
, ServerSSLConfig(..)
, ProcessMeta
, AuthProcessorResult(..)
, SslClientCertificateRequestType(..)

-- * Client Auth
, ClientSSLConfig(..)
, ClientSSLKeyCertPair(..)
, ClientMetadataCreate
, ClientMetadataCreateResult(..)
, AuthMetadataContext(..)

-- * Client
, ClientConfig(..)
, Client
, ClientCall
, ConnectivityState(..)
, clientConnectivity
, withClient
, clientRegisterMethodNormal
, clientRegisterMethodClientStreaming
, clientRegisterMethodServerStreaming
, clientRegisterMethodBiDiStreaming
, clientRequest
, clientRequestParent
, clientReader -- for server streaming
, clientWriter -- for client streaming
, clientRW     -- for bidirectional streaming
, withClientCall
, withClientCallParent
, clientCallCancel

-- * Ops
, Op(..)
, OpRecvResult(..)

-- * Streaming utilities
, StreamSend
, StreamRecv

) where

import           Network.GRPC.LowLevel.Call
import           Network.GRPC.LowLevel.Client
import           Network.GRPC.LowLevel.CompletionQueue
import           Network.GRPC.LowLevel.GRPC
import           Network.GRPC.LowLevel.Op
import           Network.GRPC.LowLevel.Server

import           Network.GRPC.Unsafe                   (ConnectivityState (..))
import           Network.GRPC.Unsafe.ChannelArgs       (Arg (..), CompressionAlgorithm (..),
                                                        CompressionLevel (..))
import           Network.GRPC.Unsafe.Op                (StatusCode (..))
import           Network.GRPC.Unsafe.Security          (AuthContext,
                                                        AuthMetadataContext (..),
                                                        AuthProcessorResult (..),
                                                        AuthProperty (..),
                                                        ClientMetadataCreate,
                                                        ClientMetadataCreateResult (..),
                                                        ProcessMeta,
                                                        SslClientCertificateRequestType (..),
                                                        addAuthProperty,
                                                        getAuthProperties)
