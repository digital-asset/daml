{-# LANGUAGE StandaloneDeriving #-}

module Network.GRPC.Unsafe.Op where

{#import Network.GRPC.Unsafe.Slice#}

import Control.Exception
import Foreign.C.Types
import Foreign.Ptr
{#import Network.GRPC.Unsafe.ByteBuffer#}
{#import Network.GRPC.Unsafe.Metadata#}

#include <grpc/grpc.h>
#include <grpc/status.h>
#include <grpc/impl/codegen/grpc_types.h>
#include <grpc_haskell.h>

{#context prefix = "grpc" #}

{#enum grpc_op_type as OpType {underscoreToCase} deriving (Eq, Show)#}
{#enum grpc_status_code as StatusCode {underscoreToCase} deriving (Eq, Read, Show)#}

-- NOTE: We don't alloc the space for the enum in Haskell because enum size is
-- implementation-dependent. See:
-- http://stackoverflow.com/questions/1113855/is-the-sizeofenum-sizeofint-always
-- | Allocates space for a 'StatusCode' and returns a pointer to it. Used to
-- receive a status code from the server with 'opRecvStatusClient'.
{#fun unsafe create_status_code_ptr as ^ {} -> `Ptr StatusCode' castPtr#}

{#fun unsafe deref_status_code_ptr as ^ {castPtr `Ptr StatusCode'} -> `StatusCode'#}

{#fun unsafe destroy_status_code_ptr as ^ {castPtr `Ptr StatusCode'} -> `()' #}

-- | Represents an array of ops to be passed to 'grpcCallStartBatch'.
-- Create an array with 'opArrayCreate', then create individual ops in the array
-- using the op* functions. For these functions, the first two arguments are
-- always the OpArray to mutate and the index in the array at which to create
-- the new op. After processing the batch and getting out any results, call
-- 'opArrayDestroy'.
{#pointer *grpc_op as OpArray newtype #}

deriving instance Show OpArray

-- | Creates an empty 'OpArray' with space for the given number of ops.
{#fun unsafe op_array_create as ^ {`Int'} -> `OpArray'#}

-- | Destroys an 'OpArray' of the given size.
{#fun unsafe op_array_destroy as ^ {`OpArray', `Int'} -> `()'#}

-- | brackets creating and destroying an 'OpArray' with the given size.
withOpArray :: Int -> (OpArray -> IO a) -> IO a
withOpArray n f = bracket (opArrayCreate n) (flip opArrayDestroy n) f

-- | Creates an op of type GRPC_OP_SEND_INITIAL_METADATA at the specified
-- index of the given 'OpArray', containing the given
-- metadata. The metadata is copied and can be destroyed after calling this
-- function.
{#fun unsafe op_send_initial_metadata as ^
  {`OpArray', `Int', `MetadataKeyValPtr', `Int'} -> `()'#}

-- | Creates an op of type GRPC_OP_SEND_INITIAL_METADATA at the specified
-- index of the given 'OpArray'. The op will contain no metadata.
{#fun unsafe op_send_initial_metadata_empty as ^ {`OpArray', `Int'} -> `()'#}

-- | Creates an op of type GRPC_OP_SEND_MESSAGE at the specified index of
-- the given 'OpArray'. The given 'ByteBuffer' is
-- copied and can be destroyed after calling this function.
{#fun unsafe op_send_message as ^ {`OpArray', `Int', `ByteBuffer'} -> `()'#}

-- | Creates an 'Op' of type GRPC_OP_SEND_CLOSE_FROM_CLIENT at the specified
-- index of the given 'OpArray'.
{#fun unsafe op_send_close_client as ^ {`OpArray', `Int'} -> `()'#}

-- | Creates an op of type GRPC_OP_RECV_INITIAL_METADATA at the specified
-- index of the given 'OpArray', and ties the given
-- 'MetadataArray' pointer to that op so that the received metadata can be
-- accessed. It is the user's responsibility to destroy the 'MetadataArray'.
{#fun unsafe op_recv_initial_metadata as ^
  {`OpArray', `Int',id `Ptr MetadataArray'} -> `()'#}

-- | Creates an op of type GRPC_OP_RECV_MESSAGE at the specified index of the
-- given 'OpArray', and ties the given
-- 'ByteBuffer' pointer to that op so that the received message can be
-- accessed. It is the user's responsibility to destroy the 'ByteBuffer'.
{#fun unsafe op_recv_message as ^ {`OpArray', `Int',id `Ptr ByteBuffer'} -> `()'#}

-- | Creates an op of type GRPC_OP_RECV_STATUS_ON_CLIENT at the specified
-- index of the given 'OpArray', and ties all the
-- input pointers to that op so that the results of the receive can be
-- accessed. It is the user's responsibility to free all the input args after
-- this call.
{#fun unsafe op_recv_status_client as ^
  {`OpArray', `Int',id `Ptr MetadataArray', castPtr `Ptr StatusCode',
   `Slice'}
  -> `()'#}

-- | Creates an op of type GRPC_OP_RECV_CLOSE_ON_SERVER at the specified index
-- of the given 'OpArray', and ties the input
-- pointer to that op so that the result of the receive can be accessed. It is
-- the user's responsibility to free the pointer.
{#fun unsafe op_recv_close_server as ^ {`OpArray', `Int', id `Ptr CInt'} -> `()'#}

-- | Creates an op of type GRPC_OP_SEND_STATUS_FROM_SERVER at the specified
-- index of the given 'OpArray'. The given
-- Metadata and string are copied when creating the op, and can be safely
-- destroyed immediately after calling this function.
{#fun unsafe op_send_status_server as ^
  {`OpArray', `Int', `Int', `MetadataKeyValPtr', `StatusCode', `Slice'}
  -> `()'#}
