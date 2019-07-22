{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Network.GRPC.Unsafe where

import Control.Exception (bracket)
import Control.Monad

import Data.ByteString (ByteString, useAsCString)
import Data.Semigroup (Semigroup)

import Foreign.C.String (CString, peekCString)
import Foreign.Marshal.Alloc (free)
import Foreign.Ptr
import Foreign.Storable
import GHC.Exts (IsString(..))

{#import Network.GRPC.Unsafe.Time#}
import Network.GRPC.Unsafe.Constants
{#import Network.GRPC.Unsafe.ByteBuffer#}
{#import Network.GRPC.Unsafe.Op#}
{#import Network.GRPC.Unsafe.Metadata#}
{#import Network.GRPC.Unsafe.ChannelArgs#}
{#import Network.GRPC.Unsafe.Slice#}

#include <grpc/grpc.h>
#include <grpc/status.h>
#include <grpc/support/alloc.h>
#include <grpc_haskell.h>

{#context prefix = "grpc" #}

newtype StatusDetails = StatusDetails {unStatusDetails :: ByteString}
  deriving (Eq, IsString, Monoid, Semigroup, Show)

{#pointer *grpc_completion_queue as CompletionQueue newtype #}

deriving instance Show CompletionQueue

-- | Represents a connection to a server. Created on the client side.
{#pointer *grpc_channel as Channel newtype #}

deriving instance Show Channel

-- | Represents a server. Created on the server side.
{#pointer *grpc_server as Server newtype #}

-- | Represents a pointer to a call. To users of the gRPC core library, this
-- type is abstract; we have no access to its fields.
{#pointer *grpc_call as Call newtype #}

deriving instance Show Call

{#pointer *grpc_call_details as CallDetails newtype #}

deriving instance Show CallDetails

{#fun unsafe create_call_details as ^ {} -> `CallDetails'#}
{#fun unsafe destroy_call_details as ^ {`CallDetails'} -> `()'#}

withCallDetails :: (CallDetails -> IO a) -> IO a
withCallDetails = bracket createCallDetails destroyCallDetails

-- instance def adapted from
-- https://mail.haskell.org/pipermail/c2hs/2007-June/000800.html
instance Storable Call where
  sizeOf (Call r) = sizeOf r
  alignment (Call r) = alignment r
  peek p = fmap Call (peek (castPtr p))
  poke p (Call r) = poke (castPtr p) r

{#enum grpc_server_register_method_payload_handling as ServerRegisterMethodPayloadHandling {underscoreToCase} deriving (Eq, Show)#}

-- | A 'Tag' is an identifier that is used with a 'CompletionQueue' to signal
-- that the corresponding operation has completed.
newtype Tag = Tag {unTag :: Ptr ()} deriving (Show, Eq)

tag :: Int -> Tag
tag = Tag . plusPtr nullPtr

noTag :: Tag
noTag = Tag nullPtr

instance Storable Tag where
  sizeOf (Tag p) = sizeOf p
  alignment (Tag p) = alignment p
  peek p = fmap Tag (peek (castPtr p))
  poke p (Tag r) = poke (castPtr p) r

-- | A 'CallHandle' is an identifier used to refer to a registered call. Create
-- one on the client with 'grpcChannelRegisterCall', and on the server with
-- 'grpcServerRegisterMethod'.
newtype CallHandle = CallHandle {unCallHandle :: Ptr ()} deriving (Show, Eq)

-- | 'Reserved' is an as-yet unused void pointer param to several gRPC
-- functions. Create one with 'reserved'.
newtype Reserved = Reserved {unReserved :: Ptr ()}

reserved :: Reserved
reserved = Reserved nullPtr

{#enum grpc_call_error as CallError {underscoreToCase} deriving (Show, Eq)#}

-- | Represents the type of a completion event on a 'CompletionQueue'.
-- 'QueueShutdown' only occurs if the queue is shutting down (e.g., when the
-- server is stopping). 'QueueTimeout' occurs when we reached the deadline
-- before receiving an 'OpComplete'.
{#enum grpc_completion_type as CompletionType {underscoreToCase}
  deriving (Show, Eq)#}

-- | Represents one event received over a 'CompletionQueue'.
data Event = Event {eventCompletionType :: CompletionType,
                    eventSuccess :: Bool,
                    eventTag :: Tag}
                    deriving (Show, Eq)

instance Storable Event where
  sizeOf _ = {#sizeof grpc_event#}
  alignment _ = {#alignof grpc_event#}
  peek p = Event <$> liftM (toEnum . fromIntegral) ({#get grpc_event->type#} p)
                 <*> liftM (> 0) ({#get grpc_event->success#} p)
                 <*> liftM Tag ({#get grpc_event->tag#} p)
  poke p (Event c s t) = do
    {#set grpc_event.type#} p $ fromIntegral $ fromEnum c
    {#set grpc_event.success#} p $ if s then 1 else 0
    {#set grpc_event.tag#} p (unTag t)

-- | Used to unwrap structs from pointers. This is all part of a workaround
-- because the C FFI can't return raw structs directly. So we wrap the C
-- function, mallocing a temporary pointer. This function peeks the struct
-- within the pointer, then frees it.
castPeek :: Storable b => Ptr a -> IO b
castPeek p = do
  val <- peek (castPtr p)
  free p
  return val

{#enum grpc_connectivity_state as ConnectivityState {underscoreToCase}
  deriving (Show, Eq)#}

{#fun grpc_init as ^ {} -> `()'#}

{#fun grpc_shutdown as ^ {} -> `()'#}

{#fun grpc_shutdown_blocking as ^ {} -> `()'#}

{#fun grpc_version_string as ^ {} -> `String' #}

-- | Create a new 'CompletionQueue' for GRPC_CQ_NEXT. See the docs for
-- 'grpcCompletionQueueShutdown' for instructions on how to clean up afterwards.
{#fun grpc_completion_queue_create_for_next as ^
  {unReserved `Reserved'} -> `CompletionQueue'#}

-- | Create a new 'CompletionQueue' for GRPC_CQ_PLUCK. See the docs for
-- 'grpcCompletionQueueShutdown' for instructions on how to clean up afterwards.
{#fun grpc_completion_queue_create_for_pluck as ^
  {unReserved `Reserved'} -> `CompletionQueue'#}

-- | Block until we get the next event off the given 'CompletionQueue',
-- using the given 'CTimeSpecPtr' as a deadline specifying the max amount of
-- time to block.
{#fun grpc_completion_queue_next_ as ^
  {`CompletionQueue', `CTimeSpecPtr',unReserved `Reserved'}
  -> `Event' castPeek*#}

-- | Block until we get the next event with the given 'Tag' off the given
-- 'CompletionQueue'. NOTE: No more than 'maxCompletionQueuePluckers' can call
-- this function concurrently!
{#fun grpc_completion_queue_pluck_ as ^
  {`CompletionQueue',unTag `Tag', `CTimeSpecPtr',unReserved `Reserved'}
  -> `Event' castPeek*#}

-- | Stops a completion queue. After all events are drained,
-- 'grpcCompletionQueueNext' will yield 'QueueShutdown' and then it is safe to
-- call 'grpcCompletionQueueDestroy'. After calling this, we must ensure no
-- new work is pushed to the queue.
{#fun grpc_completion_queue_shutdown as ^ {`CompletionQueue'} -> `()'#}

-- | Destroys a 'CompletionQueue'. See 'grpcCompletionQueueShutdown' for how to
-- use safely. Caller must ensure no threads are calling
-- 'grpcCompletionQueueNext'.
{#fun grpc_completion_queue_destroy as ^ {`CompletionQueue'} -> `()'#}

-- | Sets up a call on the client. The first string is the endpoint name (e.g.
-- @"/foo"@) and the second is the host name. In my tests so far, the host name
-- here doesn't seem to be used... it looks like the host and port specified in
-- 'grpcInsecureChannelCreate' is the one that is actually used.
{#fun grpc_channel_create_call_ as ^
  {`Channel', `Call', fromIntegral `PropagationMask', `CompletionQueue',
   useAsCString* `ByteString', useAsCString* `ByteString', `CTimeSpecPtr',unReserved `Reserved'}
  -> `Call'#}

-- | Create a channel (on the client) to the server. The first argument is
-- host and port, e.g. @"localhost:50051"@. The gRPC docs say that most clients
-- are expected to pass a 'nullPtr' for the 'ChannelArgsPtr'. We currently don't
-- expose any functions for creating channel args, since they are entirely
-- undocumented.
{#fun grpc_insecure_channel_create as ^
  {useAsCString* `ByteString', `GrpcChannelArgs', unReserved `Reserved'} -> `Channel'#}

{#fun grpc_channel_register_call as ^
  {`Channel', useAsCString* `ByteString',useAsCString* `ByteString',unReserved `Reserved'}
  -> `CallHandle' CallHandle#}

{#fun grpc_channel_create_registered_call_ as ^
  {`Channel', `Call', fromIntegral `PropagationMask', `CompletionQueue',
   unCallHandle `CallHandle', `CTimeSpecPtr', unReserved `Reserved'} -> `Call'#}

-- | get the current connectivity state of the given channel. The 'Bool' is
-- True if we should try to connect the channel.
{#fun grpc_channel_check_connectivity_state as ^
  {`Channel', `Bool'} -> `ConnectivityState'#}

-- | When the current connectivity state changes from the given
-- 'ConnectivityState', enqueues a success=1 tag on the given 'CompletionQueue'.
-- If the deadline is reached, enqueues a tag with success=0.
{#fun grpc_channel_watch_connectivity_state_ as ^
  {`Channel', `ConnectivityState', `CTimeSpecPtr', `CompletionQueue',
    unTag `Tag'}
  -> `()'#}

{#fun grpc_channel_ping as ^
  {`Channel', `CompletionQueue', unTag `Tag',unReserved `Reserved'} -> `()' #}

{#fun grpc_channel_destroy as ^ {`Channel'} -> `()'#}

-- | Starts executing a batch of ops in the given 'OpArray'. Does not block.
-- When complete, an event identified by the given 'Tag'
-- will be pushed onto the 'CompletionQueue' that was associated with the given
-- 'Call' when the 'Call' was created.
{#fun unsafe grpc_call_start_batch as ^
  {`Call', `OpArray', `Int', unTag `Tag',unReserved `Reserved'} -> `CallError'#}

{#fun grpc_call_cancel as ^ {`Call',unReserved `Reserved'} -> `()'#}

{#fun grpc_call_cancel_with_status as ^
  {`Call', `StatusCode', `String',unReserved `Reserved'} -> `()'#}

{#fun grpc_call_ref as ^ {`Call'} -> `()'#}

{#fun grpc_call_unref as ^ {`Call'} -> `()'#}

-- | Gets the peer of the current call as a string.
{#fun grpc_call_get_peer as ^ {`Call'} -> `String' getPeerPeek* #}

{#fun gpr_free as ^ {`Ptr ()'} -> `()'#}

getPeerPeek :: CString -> IO String
getPeerPeek cstr = do
  haskellStr <- peekCString cstr
  gprFree (castPtr cstr)
  return haskellStr

-- Server stuff

{#fun grpc_server_create as ^
  {`GrpcChannelArgs',unReserved `Reserved'} -> `Server'#}

{#fun grpc_server_register_method_ as ^
  {`Server',useAsCString* `ByteString',useAsCString* `ByteString', `ServerRegisterMethodPayloadHandling'} -> `CallHandle' CallHandle#}

{#fun grpc_server_register_completion_queue as ^
  {`Server', `CompletionQueue', unReserved `Reserved'} -> `()'#}

{#fun grpc_server_add_insecure_http2_port as ^
  {`Server', useAsCString* `ByteString'} -> `Int'#}

-- | Starts a server. To shut down the server, call these in order:
-- 'grpcServerShutdownAndNotify', 'grpcServerCancelAllCalls',
-- 'grpcServerDestroy'. After these are done, shut down and destroy the server's
-- completion queue with 'grpcCompletionQueueShutdown' followed by
-- 'grpcCompletionQueueDestroy'.
{#fun grpc_server_start as ^ {`Server'} -> `()'#}

{#fun grpc_server_shutdown_and_notify as ^
  {`Server', `CompletionQueue',unTag `Tag'} -> `()'#}

{#fun grpc_server_cancel_all_calls as ^
  {`Server'} -> `()'#}

-- | Destroy the server. See 'grpcServerStart' for complete shutdown
-- instructions.
{#fun grpc_server_destroy as ^ {`Server'} -> `()'#}

-- | Request a call.
-- NOTE: You need to call 'grpcCompletionQueueNext' or
-- 'grpcCompletionQueuePluck' on the completion queue with the given
-- 'Tag' before using the 'Call' pointer again.
{#fun grpc_server_request_call as ^
  {`Server',id `Ptr Call', `CallDetails', `MetadataArray',
   `CompletionQueue', `CompletionQueue',unTag `Tag'}
  -> `CallError'#}

-- | Request a registered call for the given registered method described by a
-- 'CallHandle'. The call, deadline, metadata array, and byte buffer are all
-- out parameters.
{#fun grpc_server_request_registered_call as ^
  {`Server',unCallHandle `CallHandle',id `Ptr Call', `CTimeSpecPtr',
   `MetadataArray', id `Ptr ByteBuffer', `CompletionQueue',
   `CompletionQueue',unTag `Tag'}
  -> `CallError'#}

{#fun unsafe call_details_get_method as ^ {`CallDetails'} -> `ByteString' sliceToByteString* #}

{#fun unsafe call_details_get_host as ^ {`CallDetails'} -> `ByteString' sliceToByteString* #}

{#fun call_details_get_deadline as ^ {`CallDetails'} -> `CTimeSpec' peek* #}
