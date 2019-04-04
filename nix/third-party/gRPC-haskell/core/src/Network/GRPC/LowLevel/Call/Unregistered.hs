{-# LANGUAGE RecordWildCards #-}

module Network.GRPC.LowLevel.Call.Unregistered where

import qualified Network.GRPC.LowLevel.Call                     as Reg
import           Network.GRPC.LowLevel.CompletionQueue
import           Network.GRPC.LowLevel.GRPC                     (MetadataMap,
                                                                 grpcDebug)
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.Op                         as C
import           System.Clock                                   (TimeSpec)

-- | Represents one unregistered GRPC call on the server.  Contains pointers to
-- all the C state needed to respond to an unregistered call.
data ServerCall = ServerCall
  { unsafeSC            :: C.Call
  , callCQ              :: CompletionQueue
  , metadata            :: MetadataMap
  , callDeadline        :: TimeSpec
  , callMethod          :: Reg.MethodName
  , callHost            :: Reg.Host
  }

convertCall :: ServerCall -> Reg.ServerCall ()
convertCall ServerCall{..} =
  Reg.ServerCall unsafeSC callCQ metadata () callDeadline

serverCallCancel :: ServerCall -> C.StatusCode -> String -> IO ()
serverCallCancel sc code reason =
  C.grpcCallCancelWithStatus (unsafeSC sc) code reason C.reserved

debugServerCall :: ServerCall -> IO ()
#ifdef DEBUG
debugServerCall ServerCall{..} = do
  let C.Call ptr = unsafeSC
      dbug = grpcDebug . ("debugServerCall(U): " ++)

  dbug $ "server call: " ++ show ptr
  dbug $ "metadata: "    ++ show metadata

  dbug $ "deadline: " ++ show callDeadline
  dbug $ "method: "   ++ show callMethod
  dbug $ "host: "     ++ show callHost
#else
{-# INLINE debugServerCall #-}
debugServerCall = const $ return ()
#endif

destroyServerCall :: ServerCall -> IO ()
destroyServerCall call@ServerCall{..} = do
  grpcDebug "destroyServerCall(U): entered."
  debugServerCall call
  grpcDebug $ "Destroying server-side call object: " ++ show unsafeSC
  C.grpcCallUnref unsafeSC
