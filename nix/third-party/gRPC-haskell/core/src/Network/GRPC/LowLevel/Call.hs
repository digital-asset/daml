{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}

-- | This module defines data structures and operations pertaining to registered
-- calls; for unregistered call support, see
-- `Network.GRPC.LowLevel.Call.Unregistered`.
module Network.GRPC.LowLevel.Call where

import           Control.Monad.Managed                          (Managed, managed)
import           Control.Exception                              (bracket)
import           Data.ByteString                                (ByteString)
import           Data.ByteString.Char8                          (pack)
import           Data.List                                      (intersperse)
import           Data.String                                    (IsString)
import           Foreign.Marshal.Alloc                          (free, malloc)
import           Foreign.Ptr                                    (Ptr, nullPtr)
import           Foreign.Storable                               (Storable, peek)
import           Network.GRPC.LowLevel.CompletionQueue.Internal
import           Network.GRPC.LowLevel.GRPC                     (MetadataMap,
                                                                 grpcDebug)
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.ByteBuffer                 as C
import qualified Network.GRPC.Unsafe.Op                         as C
import           System.Clock

-- | Models the four types of RPC call supported by gRPC (and correspond to
-- DataKinds phantom types on RegisteredMethods).
data GRPCMethodType
  = Normal
  | ClientStreaming
  | ServerStreaming
  | BiDiStreaming
  deriving (Show, Eq, Ord, Enum)

type family MethodPayload a where
  MethodPayload 'Normal = ByteString
  MethodPayload 'ClientStreaming = ()
  MethodPayload 'ServerStreaming = ByteString
  MethodPayload 'BiDiStreaming = ()

--TODO: try replacing this class with a plain old function so we don't have the
-- Payloadable constraint everywhere.

extractPayload :: RegisteredMethod mt
                  -> Ptr C.ByteBuffer
                  -> IO (MethodPayload mt)
extractPayload (RegisteredMethodNormal _ _ _) p =
  peek p >>= C.copyByteBufferToByteString
extractPayload (RegisteredMethodClientStreaming _ _ _) _ = return ()
extractPayload (RegisteredMethodServerStreaming _ _ _) p =
  peek p >>= C.copyByteBufferToByteString
extractPayload (RegisteredMethodBiDiStreaming _ _ _) _ = return ()

newtype MethodName = MethodName {unMethodName :: ByteString}
  deriving (Show, Eq, IsString)

newtype Host = Host {unHost :: ByteString}
  deriving (Show, Eq, IsString)

newtype Port = Port {unPort :: Int}
  deriving (Eq, Num, Show)

newtype Endpoint = Endpoint {unEndpoint :: ByteString}
  deriving (Show, Eq, IsString)

-- | Given a hostname and port, produces a "host:port" string
endpoint :: Host -> Port -> Endpoint
endpoint (Host h) (Port p) = Endpoint (h <> ":" <> pack (show p))

-- | Represents a registered method. Methods can optionally be registered in
-- order to make the C-level request/response code simpler.  Before making or
-- awaiting a registered call, the method must be registered with the client
-- (see 'clientRegisterMethod') and the server (see 'serverRegisterMethod').
-- Contains state for identifying that method in the underlying gRPC
-- library. Note that we use a DataKind-ed phantom type to help constrain use of
-- different kinds of registered methods.
data RegisteredMethod (mt :: GRPCMethodType) where
  RegisteredMethodNormal :: MethodName
                            -> Endpoint
                            -> C.CallHandle
                            -> RegisteredMethod 'Normal
  RegisteredMethodClientStreaming :: MethodName
                                     -> Endpoint
                                     -> C.CallHandle
                                     -> RegisteredMethod 'ClientStreaming
  RegisteredMethodServerStreaming :: MethodName
                                     -> Endpoint
                                     -> C.CallHandle
                                     -> RegisteredMethod 'ServerStreaming
  RegisteredMethodBiDiStreaming :: MethodName
                                   -> Endpoint
                                   -> C.CallHandle
                                   -> RegisteredMethod 'BiDiStreaming

instance Show (RegisteredMethod a) where
  show (RegisteredMethodNormal x y z) =
    "RegisteredMethodNormal "
    ++ concat (intersperse " " [show x, show y, show z])
  show (RegisteredMethodClientStreaming x y z) =
    "RegisteredMethodClientStreaming "
    ++ concat (intersperse " " [show x, show y, show z])
  show (RegisteredMethodServerStreaming x y z) =
    "RegisteredMethodServerStreaming "
    ++ concat (intersperse " " [show x, show y, show z])
  show (RegisteredMethodBiDiStreaming x y z) =
    "RegisteredMethodBiDiStreaming "
    ++ concat (intersperse " " [show x, show y, show z])

methodName :: RegisteredMethod mt -> MethodName
methodName (RegisteredMethodNormal x _ _) = x
methodName (RegisteredMethodClientStreaming x _ _) = x
methodName (RegisteredMethodServerStreaming x _ _) = x
methodName (RegisteredMethodBiDiStreaming x _ _) = x

methodEndpoint :: RegisteredMethod mt -> Endpoint
methodEndpoint (RegisteredMethodNormal _ x _) = x
methodEndpoint (RegisteredMethodClientStreaming _ x _) = x
methodEndpoint (RegisteredMethodServerStreaming _ x _) = x
methodEndpoint (RegisteredMethodBiDiStreaming _ x _) = x

methodHandle :: RegisteredMethod mt -> C.CallHandle
methodHandle (RegisteredMethodNormal _ _ x) = x
methodHandle (RegisteredMethodClientStreaming _ _ x) = x
methodHandle (RegisteredMethodServerStreaming _ _ x) = x
methodHandle (RegisteredMethodBiDiStreaming _ _ x) = x

methodType :: RegisteredMethod mt -> GRPCMethodType
methodType (RegisteredMethodNormal _ _ _)          = Normal
methodType (RegisteredMethodClientStreaming _ _ _) = ClientStreaming
methodType (RegisteredMethodServerStreaming _ _ _) = ServerStreaming
methodType (RegisteredMethodBiDiStreaming _ _ _)   = BiDiStreaming

-- | Represents one GRPC call (i.e. request) on the client.
-- This is used to associate send/receive 'Op's with a request.
data ClientCall = ClientCall { unsafeCC :: C.Call }

clientCallCancel :: ClientCall -> IO ()
clientCallCancel cc = C.grpcCallCancel (unsafeCC cc) C.reserved

-- | Represents one registered GRPC call on the server. Contains pointers to all
-- the C state needed to respond to a registered call.
data ServerCall a = ServerCall
  { unsafeSC            :: C.Call
  , callCQ              :: CompletionQueue
  , metadata            :: MetadataMap
  , payload             :: a
  , callDeadline        :: TimeSpec
  } deriving (Functor, Show)

serverCallCancel :: ServerCall a -> C.StatusCode -> String -> IO ()
serverCallCancel sc code reason =
  C.grpcCallCancelWithStatus (unsafeSC sc) code reason C.reserved

-- | NB: For now, we've assumed that the method type is all the info we need to
-- decide the server payload handling method.
payloadHandling :: GRPCMethodType -> C.ServerRegisterMethodPayloadHandling
payloadHandling Normal          = C.SrmPayloadReadInitialByteBuffer
payloadHandling ClientStreaming = C.SrmPayloadNone
payloadHandling ServerStreaming = C.SrmPayloadReadInitialByteBuffer
payloadHandling BiDiStreaming   = C.SrmPayloadNone

-- | Optionally allocate a managed byte buffer for a payload, depending on the
-- given method type. If no payload is needed, the returned pointer is null
mgdPayload :: GRPCMethodType -> Managed (Ptr C.ByteBuffer)
mgdPayload mt
  | payloadHandling mt == C.SrmPayloadNone = return nullPtr
  | otherwise                              = managed C.withByteBufferPtr

mgdPtr :: forall a. Storable a => Managed (Ptr a)
mgdPtr = managed (bracket malloc free)

serverCallIsExpired :: ServerCall a -> IO Bool
serverCallIsExpired sc = do
  currTime <- getTime Monotonic
  return $ currTime > (callDeadline sc)

debugClientCall :: ClientCall -> IO ()
{-# INLINE debugClientCall #-}
#ifdef DEBUG
debugClientCall (ClientCall (C.Call ptr)) =
  grpcDebug $ "debugCall: client call: " ++ (show ptr)
#else
debugClientCall = const $ return ()
#endif

debugServerCall :: ServerCall a -> IO ()
#ifdef DEBUG
debugServerCall sc@(ServerCall (C.Call ptr) _ _ _ _) = do
  let dbug = grpcDebug . ("debugServerCall(R): " ++)
  dbug $ "server call: "  ++ show ptr
  dbug $ "callCQ: "       ++ show (callCQ sc)
  dbug $ "metadata: " ++ show (metadata sc)
  dbug $ "deadline ptr: " ++ show (callDeadline sc)
#else
{-# INLINE debugServerCall #-}
debugServerCall = const $ return ()
#endif

destroyClientCall :: ClientCall -> IO ()
destroyClientCall cc = do
  grpcDebug "Destroying client-side call object."
  C.grpcCallUnref (unsafeCC cc)

destroyServerCall :: ServerCall a -> IO ()
destroyServerCall sc@ServerCall{ unsafeSC = c, .. } = do
  grpcDebug "destroyServerCall(R): entered."
  debugServerCall sc
  grpcDebug $ "Destroying server-side call object: " ++ show c
  C.grpcCallUnref c
