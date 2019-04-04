{-# LANGUAGE RecordWildCards #-}

module Network.GRPC.LowLevel.CompletionQueue.Internal where

import           Control.Concurrent.STM        (atomically, retry, check)
import           Control.Concurrent.STM.TVar   (TVar, modifyTVar', readTVar,
                                                writeTVar)
import           Control.Exception             (bracket)
import           Control.Monad
import           Data.IORef                    (IORef, atomicModifyIORef')
import           Foreign.Ptr                   (nullPtr, plusPtr)
import           Network.GRPC.LowLevel.GRPC
import qualified Network.GRPC.Unsafe           as C
import qualified Network.GRPC.Unsafe.Constants as C
import qualified Network.GRPC.Unsafe.Time      as C
import           System.Timeout                                 (timeout)

-- NOTE: the concurrency requirements for a CompletionQueue are a little
-- complicated. There are two read operations: next and pluck. We can either
-- call next on a CQ or call pluck up to 'maxCompletionQueuePluckers' times
-- concurrently, but we can't mix next and pluck calls. Fortunately, we only
-- need to use next when we are shutting down the queue. Thus, we do two things
-- to shut down:
-- 1. Set the shuttingDown 'TVar' to 'True'. When this is set, no new pluck
--    calls will be allowed to start.
-- 2. Wait until no threads are plucking, as counted by 'currentPluckers'.
-- This logic can be seen in 'pluck' and 'shutdownCompletionQueue'.

-- NOTE: There is one more possible race condition: pushing work onto the queue
-- after we begin to shut down.
-- Solution: another counter, which must reach zero before the shutdown
-- can start.

-- TODO: 'currentPushers' currently imposes an arbitrary limit on the number of
-- concurrent pushers to the CQ, but I don't know what the limit should be set
-- to. I haven't found any documentation that suggests there is a limit imposed
-- by the gRPC library, but there might be. Need to investigate further.

-- | Wraps the state necessary to use a gRPC completion queue. Completion queues
-- are used to wait for batches gRPC operations ('Op's) to finish running, as
-- well as wait for various other operations, such as server shutdown, pinging,
-- checking to see if we've been disconnected, and so forth.
data CompletionQueue = CompletionQueue {unsafeCQ        :: C.CompletionQueue,
                                        -- ^ All access to this field must be
                                        -- guarded by a check of 'shuttingDown'.
                                        currentPluckers :: TVar Int,
                                        -- ^ Used to limit the number of
                                        -- concurrent calls to pluck on this
                                        -- queue.
                                        -- The max value is set by gRPC in
                                        -- 'C.maxCompletionQueuePluckers'
                                        currentPushers  :: TVar Int,
                                        -- ^ Used to prevent new work from
                                        -- being pushed onto the queue when
                                        -- the queue begins to shut down.
                                        shuttingDown    :: TVar Bool,
                                        -- ^ Used to prevent new pluck calls on
                                        -- the queue when the queue begins to
                                        -- shut down.
                                        nextTag         :: IORef Int
                                        -- ^ Used to supply unique tags for work
                                        -- items pushed onto the queue.
                                       }

instance Show CompletionQueue where show = show . unsafeCQ

type TimeoutSeconds = Int

data CQOpType = Push | Pluck deriving (Show, Eq, Enum)

-- | Create a new 'C.Tag' for identifying work items on the 'CompletionQueue'.
-- This will eventually wrap around after reaching @maxBound :: Int@, but from a
-- practical perspective, that should be safe.
newTag :: CompletionQueue -> IO C.Tag
newTag CompletionQueue{..} = do
  i <- atomicModifyIORef' nextTag (\i -> (i+1,i))
  return $ C.Tag $ plusPtr nullPtr i

-- | Safely brackets an operation that pushes work onto or plucks results from
-- the given 'CompletionQueue'.
withPermission :: CQOpType
               -> CompletionQueue
               -> IO (Either GRPCIOError a)
               -> IO (Either GRPCIOError a)
withPermission op cq act = bracket acquire release $ \gotResource ->
  if gotResource then act else return (Left GRPCIOShutdown)
  where
    acquire = atomically $ do
      isShuttingDown <- readTVar (shuttingDown cq)
      unless isShuttingDown $ do
        currCount <- readTVar (getCount op cq)
        if currCount < getLimit op
          then writeTVar (getCount op cq) (currCount + 1)
          else retry
      return (not isShuttingDown)
    release gotResource = when gotResource $
      atomically $ modifyTVar' (getCount op cq) (subtract 1)

-- | Waits for the given number of seconds for the given tag to appear on the
-- completion queue. Throws 'GRPCIOShutdown' if the completion queue is shutting
-- down and cannot handle new requests. Note that the timeout is optional. When
-- doing client ops, provide @Nothing@ and the pluck will automatically fail if
-- the deadline associated with the 'ClientCall' expires. If plucking
-- 'serverRequestCall', this will block forever unless a timeout is given.
pluck :: CompletionQueue -> C.Tag -> Maybe TimeoutSeconds
         -> IO (Either GRPCIOError ())
pluck cq@CompletionQueue{..} tag mwait = do
  grpcDebug $ "pluck: called with tag=" ++ show tag ++ ",mwait=" ++ show mwait
  withPermission Pluck cq $ pluck' cq tag mwait

-- Variant of pluck' which assumes pluck permission has been granted.
pluck' :: CompletionQueue
       -> C.Tag
       -> Maybe TimeoutSeconds
       -> IO (Either GRPCIOError ())
pluck' CompletionQueue{..} tag mwait =
  maybe C.withInfiniteDeadline C.withDeadlineSeconds mwait $ \dead -> do
    grpcDebug $ "pluck: blocking on grpc_completion_queue_pluck for tag=" ++ show tag
    ev <- C.grpcCompletionQueuePluck unsafeCQ tag dead C.reserved
    grpcDebug $ "pluck finished: " ++ show ev
    return $ if isEventSuccessful ev then Right () else eventToError ev

-- | Translate 'C.Event' to an error. The caller is responsible for ensuring
-- that the event actually corresponds to an error condition; a successful event
-- will be translated to a 'GRPCIOUnknownError'.
eventToError :: C.Event -> (Either GRPCIOError a)
eventToError (C.Event C.QueueShutdown _ _) = Left GRPCIOShutdown
eventToError (C.Event C.QueueTimeout _ _) = Left GRPCIOTimeout
eventToError (C.Event C.OpComplete False _) = Left GRPCIOTimeout
eventToError _ = Left GRPCIOUnknownError

-- | Returns true iff the given grpc_event was a success.
isEventSuccessful :: C.Event -> Bool
isEventSuccessful (C.Event C.OpComplete True _) = True
isEventSuccessful _ = False

maxWorkPushers :: Int
maxWorkPushers = 100 --TODO: figure out what this should be.

getCount :: CQOpType -> CompletionQueue -> TVar Int
getCount Push  = currentPushers
getCount Pluck = currentPluckers

getLimit :: CQOpType -> Int
getLimit Push  = maxWorkPushers
getLimit Pluck = C.maxCompletionQueuePluckers

-- | Shuts down the completion queue. See the comment above 'CompletionQueue'
-- for the strategy we use to ensure that no one tries to use the
-- queue after we begin the shutdown process. Errors with
-- 'GRPCIOShutdownFailure' if the queue can't be shut down within 5 seconds.
shutdownCompletionQueue :: CompletionQueue -> IO (Either GRPCIOError ())
shutdownCompletionQueue CompletionQueue{..} = do
  atomically $ writeTVar shuttingDown True
  atomically $ do
    readTVar currentPushers  >>= check . (==0)
    readTVar currentPluckers >>= check . (==0)
  --drain the queue
  C.grpcCompletionQueueShutdown unsafeCQ
  loopRes <- timeout (5*10^(6::Int)) drainLoop
  grpcDebug $ "Got CQ loop shutdown result of: " ++ show loopRes
  case loopRes of
    Nothing -> return $ Left GRPCIOShutdownFailure
    Just () -> C.grpcCompletionQueueDestroy unsafeCQ >> return (Right ())

  where drainLoop :: IO ()
        drainLoop = do
          grpcDebug "drainLoop: before next() call"
          ev <- C.withDeadlineSeconds 1 $ \deadline ->
                  C.grpcCompletionQueuePluck unsafeCQ C.noTag deadline C.reserved
          grpcDebug $ "drainLoop: next() call got " ++ show ev
          case C.eventCompletionType ev of
            C.QueueShutdown -> return ()
            C.QueueTimeout -> drainLoop
            C.OpComplete -> drainLoop
