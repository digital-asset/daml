{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE PatternSynonyms            #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ViewPatterns               #-}

module Network.GRPC.LowLevel.Op where

import           Control.Exception
import           Control.Monad
import           Control.Monad.Trans.Except
import           Data.ByteString                       (ByteString)
import qualified Data.ByteString                       as B
import           Data.Maybe                            (catMaybes)
import           Foreign.C.Types                       (CInt)
import           Foreign.Marshal.Alloc                 (free, malloc)
import           Foreign.Ptr                           (Ptr, nullPtr)
import           Foreign.Storable                      (peek)
import           Network.GRPC.LowLevel.CompletionQueue
import           Network.GRPC.LowLevel.GRPC
import qualified Network.GRPC.Unsafe                   as C (Call)
import qualified Network.GRPC.Unsafe.ByteBuffer        as C
import qualified Network.GRPC.Unsafe.Metadata          as C
import qualified Network.GRPC.Unsafe.Op                as C
import qualified Network.GRPC.Unsafe.Slice             as C
import           Network.GRPC.Unsafe.Slice             (Slice)

-- | Sum describing all possible send and receive operations that can be batched
-- and executed by gRPC. Usually these are processed in a handful of
-- combinations depending on the 'MethodType' of the call being run.
data Op = OpSendInitialMetadata MetadataMap
          | OpSendMessage B.ByteString
          | OpSendCloseFromClient
          | OpSendStatusFromServer MetadataMap C.StatusCode StatusDetails
          | OpRecvInitialMetadata
          | OpRecvMessage
          | OpRecvStatusOnClient
          | OpRecvCloseOnServer
          deriving (Show)

-- | Container holding the pointers to the C and gRPC data needed to execute the
-- corresponding 'Op'. These are obviously unsafe, and should only be used with
-- 'withOpContexts'.
data OpContext =
  OpSendInitialMetadataContext C.MetadataKeyValPtr Int
  | OpSendMessageContext (C.ByteBuffer, C.Slice)
  | OpSendCloseFromClientContext
  | OpSendStatusFromServerContext C.MetadataKeyValPtr Int C.StatusCode Slice
  | OpRecvInitialMetadataContext (Ptr C.MetadataArray)
  | OpRecvMessageContext (Ptr C.ByteBuffer)
  | OpRecvStatusOnClientContext (Ptr C.MetadataArray) (Ptr C.StatusCode) Slice
  | OpRecvCloseOnServerContext (Ptr CInt)
  deriving Show

-- | Length we pass to gRPC for receiving status details
-- when processing 'OpRecvStatusOnClient'. It appears that gRPC actually ignores
-- this length and reallocates a longer string if necessary.
defaultStatusStringLen :: Int
defaultStatusStringLen = 128

-- | Allocates and initializes the 'Opcontext' corresponding to the given 'Op'.
createOpContext :: Op -> IO OpContext
createOpContext (OpSendInitialMetadata m) =
  uncurry OpSendInitialMetadataContext <$> C.createMetadata m
createOpContext (OpSendMessage bs) =
  fmap OpSendMessageContext (C.createByteBuffer bs)
createOpContext (OpSendCloseFromClient) = return OpSendCloseFromClientContext
createOpContext (OpSendStatusFromServer m code (StatusDetails str)) =
  uncurry OpSendStatusFromServerContext
  <$> C.createMetadata m
  <*> return code
  <*> C.byteStringToSlice str
createOpContext OpRecvInitialMetadata =
  fmap OpRecvInitialMetadataContext C.metadataArrayCreate
createOpContext OpRecvMessage =
  fmap OpRecvMessageContext C.createReceivingByteBuffer
createOpContext OpRecvStatusOnClient = do
  pmetadata <- C.metadataArrayCreate
  pstatus <- C.createStatusCodePtr
  slice <- C.grpcSliceMalloc defaultStatusStringLen
  return $ OpRecvStatusOnClientContext pmetadata pstatus slice
createOpContext OpRecvCloseOnServer =
  fmap OpRecvCloseOnServerContext $ malloc

-- | Mutates the given raw array of ops at the given index according to the
-- given 'OpContext'.
setOpArray :: C.OpArray -> Int -> OpContext -> IO ()
setOpArray arr i (OpSendInitialMetadataContext kvs l) =
  C.opSendInitialMetadata arr i kvs l
setOpArray arr i (OpSendMessageContext (bb,_)) =
  C.opSendMessage arr i bb
setOpArray arr i OpSendCloseFromClientContext =
  C.opSendCloseClient arr i
setOpArray arr i (OpSendStatusFromServerContext kvs l code details) =
    C.opSendStatusServer arr i l kvs code details
setOpArray arr i (OpRecvInitialMetadataContext pmetadata) =
  C.opRecvInitialMetadata arr i pmetadata
setOpArray arr i (OpRecvMessageContext pbb) =
  C.opRecvMessage arr i pbb
setOpArray arr i (OpRecvStatusOnClientContext pmetadata pstatus slice) =
  C.opRecvStatusClient arr i pmetadata pstatus slice
setOpArray arr i (OpRecvCloseOnServerContext pcancelled) = do
  C.opRecvCloseServer arr i pcancelled

-- | Cleans up an 'OpContext'.
freeOpContext :: OpContext -> IO ()
freeOpContext (OpSendInitialMetadataContext m _) = C.metadataFree m
freeOpContext (OpSendMessageContext (bb, s)) =
  C.grpcByteBufferDestroy bb >> C.freeSlice s
freeOpContext OpSendCloseFromClientContext = return ()
freeOpContext (OpSendStatusFromServerContext metadata _ _ _) =
  C.metadataFree metadata
freeOpContext (OpRecvInitialMetadataContext metadata) =
  C.metadataArrayDestroy metadata
freeOpContext (OpRecvMessageContext pbb) =
  C.destroyReceivingByteBuffer pbb
freeOpContext (OpRecvStatusOnClientContext metadata pcode slice) = do
  C.metadataArrayDestroy metadata
  C.destroyStatusCodePtr pcode
  C.freeSlice slice
freeOpContext (OpRecvCloseOnServerContext pcancelled) =
  grpcDebug ("freeOpContext: freeing pcancelled: " ++ show pcancelled)
  >> free pcancelled

-- | Allocates an `OpArray` and a list of `OpContext`s from the given list of
-- `Op`s.
withOpArrayAndCtxts :: [Op] -> ((C.OpArray, [OpContext]) -> IO a) -> IO a
withOpArrayAndCtxts ops = bracket setup teardown
  where setup = do ctxts <- mapM createOpContext ops
                   let l = length ops
                   arr <- C.opArrayCreate l
                   sequence_ $ zipWith (setOpArray arr) [0..l-1] ctxts
                   return (arr, ctxts)
        teardown (arr, ctxts) = do C.opArrayDestroy arr (length ctxts)
                                   mapM_ freeOpContext ctxts

-- | Container holding GC-managed results for 'Op's which receive data.
data OpRecvResult =
  OpRecvInitialMetadataResult MetadataMap
  | OpRecvMessageResult (Maybe B.ByteString)
    -- ^ If a streaming call is in progress and the stream terminates normally,
    -- or If the client or server dies, we might not receive a response body, in
    -- which case this will be 'Nothing'.
  | OpRecvStatusOnClientResult MetadataMap C.StatusCode B.ByteString
  | OpRecvCloseOnServerResult Bool -- ^ True if call was cancelled.
  deriving (Show)

-- | For the given 'OpContext', if the 'Op' receives data, copies the data out
-- of the 'OpContext' and into GC-managed Haskell types. After this, it is safe
-- to destroy the 'OpContext'.
resultFromOpContext :: OpContext -> IO (Maybe OpRecvResult)
resultFromOpContext (OpRecvInitialMetadataContext pmetadata) = do
  grpcDebug "resultFromOpContext: OpRecvInitialMetadataContext"
  metadata <- peek pmetadata
  metadataMap <- C.getAllMetadataArray metadata
  return $ Just $ OpRecvInitialMetadataResult metadataMap
resultFromOpContext (OpRecvMessageContext pbb) = do
  grpcDebug "resultFromOpContext: OpRecvMessageContext"
  bb@(C.ByteBuffer bbptr) <- peek pbb
  if bbptr == nullPtr
     then do grpcDebug "resultFromOpContext: WARNING: got empty message."
             return $ Just $ OpRecvMessageResult Nothing
     else do bs <- C.copyByteBufferToByteString bb
             grpcDebug $ "resultFromOpContext: bb copied: " ++ show bs
             return $ Just $ OpRecvMessageResult (Just bs)
resultFromOpContext (OpRecvStatusOnClientContext pmetadata pcode pstr) = do
  grpcDebug "resultFromOpContext: OpRecvStatusOnClientContext"
  metadata <- peek pmetadata
  metadataMap <- C.getAllMetadataArray metadata
  code <- C.derefStatusCodePtr pcode
  statusInfo <- C.sliceToByteString pstr
  return $ Just $ OpRecvStatusOnClientResult metadataMap code statusInfo
resultFromOpContext (OpRecvCloseOnServerContext pcancelled) = do
  grpcDebug "resultFromOpContext: OpRecvCloseOnServerContext"
  cancelled <- fmap (\x -> if x > 0 then True else False)
                    (peek pcancelled)
  return $ Just $ OpRecvCloseOnServerResult cancelled
resultFromOpContext _ = do
  grpcDebug "resultFromOpContext: saw non-result op type."
  return Nothing

-- | For a given call, run the given 'Op's on the given completion queue with
-- the given tag. Blocks until the ops are complete or the deadline on the
-- associated call has been reached.
-- TODO: now that we distinguish between different types
-- of calls at the type level, we could try to limit the input 'Op's more
-- appropriately. E.g., we don't use an 'OpRecvInitialMetadata' when receiving a
-- registered call, because gRPC handles that for us.

-- TODO: the list of 'Op's type is less specific than it could be. There are
-- only a few different sequences of 'Op's we will see in practice. Once we
-- figure out what those are, we should create a more specific sum
-- type. However, since ops can fail, the list of 'OpRecvResult' returned by
-- 'runOps' can vary in their contents and are perhaps less amenable to
-- simplification.  In the meantime, from looking at the core tests, it looks
-- like it is safe to say that we always get a
-- GRPC_CALL_ERROR_TOO_MANY_OPERATIONS error if we use the same 'Op' twice in
-- the same batch, so we might want to change the list to a set. I don't think
-- order matters within a batch. Need to check.
runOps :: C.Call
          -- ^ 'Call' that this batch is associated with. One call can be
          -- associated with many batches.
       -> CompletionQueue
          -- ^ Queue on which our tag will be placed once our ops are done
          -- running.
       -> [Op]
          -- ^ The list of 'Op's to execute.
       -> IO (Either GRPCIOError [OpRecvResult])
runOps call cq ops =
  let l = length ops in
    withOpArrayAndCtxts ops $ \(opArray, contexts) -> do
      grpcDebug $ "runOps: allocated op contexts: " ++ show contexts
      tag <- newTag cq
      grpcDebug $ "runOps: tag: " ++ show tag
      callError <- startBatch cq call opArray l tag
      grpcDebug $ "runOps: called start_batch. callError: "
                   ++ (show callError)
      case callError of
        Left x -> return $ Left x
        Right () -> do
          ev <- pluck cq tag Nothing
          grpcDebug $ "runOps: pluck returned " ++ show ev
          case ev of
            Right () -> do
              grpcDebug "runOps: got good op; starting."
              fmap (Right . catMaybes) $ mapM resultFromOpContext contexts
            Left err -> return $ Left err

runOps' :: C.Call
        -> CompletionQueue
        -> [Op]
        -> ExceptT GRPCIOError IO [OpRecvResult]
runOps' c cq = ExceptT . runOps c cq

-- | If response status info is present in the given 'OpRecvResult's, returns
-- a tuple of trailing metadata, status code, and status details.
extractStatusInfo :: [OpRecvResult]
                     -> Maybe (MetadataMap, C.StatusCode, B.ByteString)
extractStatusInfo [] = Nothing
extractStatusInfo (OpRecvStatusOnClientResult meta code details:_) =
  Just (meta, code, details)
extractStatusInfo (_:xs) = extractStatusInfo xs

--------------------------------------------------------------------------------
-- Types and helpers for common ops batches

type SendSingle a
  =  C.Call
  -> CompletionQueue
  -> a
  -> ExceptT GRPCIOError IO ()

type RecvSingle a
  =  C.Call
  -> CompletionQueue
  -> ExceptT GRPCIOError IO a

pattern RecvMsgRslt :: Maybe ByteString -> Either a [OpRecvResult]
pattern RecvMsgRslt mmsg <- Right [OpRecvMessageResult mmsg]

sendSingle :: SendSingle Op
sendSingle c cq op = void (runOps' c cq [op])

sendInitialMetadata :: SendSingle MetadataMap
sendInitialMetadata c cq = sendSingle c cq . OpSendInitialMetadata

sendStatusFromServer :: SendSingle (MetadataMap, C.StatusCode, StatusDetails)
sendStatusFromServer c cq (md, st, ds) =
  sendSingle c cq (OpSendStatusFromServer md st ds)

recvInitialMessage :: RecvSingle ByteString
recvInitialMessage c cq = ExceptT (streamRecvPrim c cq ) >>= \case
  Nothing -> throwE (GRPCIOInternalUnexpectedRecv "recvInitialMessage: no message.")
  Just bs -> return bs

recvInitialMetadata :: RecvSingle MetadataMap
recvInitialMetadata c cq = runOps' c cq [OpRecvInitialMetadata] >>= \case
  [OpRecvInitialMetadataResult md]
    -> return md
  _ -> throwE (GRPCIOInternalUnexpectedRecv "recvInitialMetadata")

recvInitialMsgMD :: RecvSingle (Maybe ByteString, MetadataMap)
recvInitialMsgMD c cq = runOps' c cq [OpRecvInitialMetadata, OpRecvMessage] >>= \case
  [ OpRecvInitialMetadataResult md, OpRecvMessageResult mmsg]
    -> return (mmsg, md)
  _ -> throwE (GRPCIOInternalUnexpectedRecv "recvInitialMsgMD")

recvStatusOnClient :: RecvSingle (MetadataMap, C.StatusCode, StatusDetails)
recvStatusOnClient c cq = runOps' c cq [OpRecvStatusOnClient] >>= \case
  [OpRecvStatusOnClientResult md st ds]
    -> return (md, st, StatusDetails ds)
  _ -> throwE (GRPCIOInternalUnexpectedRecv "recvStatusOnClient")


--------------------------------------------------------------------------------
-- Streaming types and helpers

type StreamRecv a = IO (Either GRPCIOError (Maybe a))
streamRecvPrim :: C.Call -> CompletionQueue -> StreamRecv ByteString
streamRecvPrim c cq = f <$> runOps c cq [OpRecvMessage]
  where
    f (RecvMsgRslt mmsg) = Right mmsg
    f Right{}            = Left (GRPCIOInternalUnexpectedRecv "streamRecvPrim")
    f (Left e)           = Left e

type StreamSend a = a -> IO (Either GRPCIOError ())
streamSendPrim :: C.Call -> CompletionQueue -> StreamSend ByteString
streamSendPrim c cq bs = f <$> runOps c cq [OpSendMessage bs]
  where
    f (Right []) = Right ()
    f Right{}    = Left (GRPCIOInternalUnexpectedRecv "streamSendPrim")
    f (Left e)   = Left e

type WritesDone = IO (Either GRPCIOError ())
writesDonePrim :: C.Call -> CompletionQueue -> WritesDone
writesDonePrim c cq = f <$> runOps c cq [OpSendCloseFromClient]
  where
    f (Right []) = Right ()
    f Right{}    = Left (GRPCIOInternalUnexpectedRecv "writesDonePrim")
    f (Left e)   = Left e
