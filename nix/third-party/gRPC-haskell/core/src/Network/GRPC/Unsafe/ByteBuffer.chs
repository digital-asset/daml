{-# LANGUAGE StandaloneDeriving #-}

module Network.GRPC.Unsafe.ByteBuffer where

#include <grpc/grpc.h>
#include <grpc/slice.h>
#include <grpc/impl/codegen/compression_types.h>
#include <grpc/slice_buffer.h>

#include <grpc_haskell.h>

{#import Network.GRPC.Unsafe.Slice#}
{#import Network.GRPC.Unsafe.ChannelArgs#}
import Control.Exception (bracket)
import qualified Data.ByteString as B
import Foreign.Ptr
import Foreign.C.Types
import Foreign.Storable

-- | Represents a pointer to a gRPC byte buffer containing 1 or more 'Slice's.
-- Must be destroyed manually with 'grpcByteBufferDestroy'.
{#pointer *grpc_byte_buffer as ByteBuffer newtype #}

deriving instance Show ByteBuffer

--Trivial Storable instance because 'ByteBuffer' type is a pointer.
instance Storable ByteBuffer where
  sizeOf (ByteBuffer r) = sizeOf r
  alignment (ByteBuffer r) = alignment r
  peek p = fmap ByteBuffer (peek (castPtr p))
  poke p (ByteBuffer r) = poke (castPtr p) r

--TODO: When I switched this to a ForeignPtr with a finalizer, I got errors
--about freeing un-malloced memory. Calling the same destroy function by hand
--works fine in the same code, though. Until I find a workaround, going to free
--everything by hand.

-- | Represents a pointer to a ByteBufferReader. Must be destroyed manually with
-- 'byteBufferReaderDestroy'.
{#pointer *grpc_byte_buffer_reader as ByteBufferReader newtype #}

-- | Creates a pointer to a 'ByteBuffer'. This is used to receive data when
-- creating a GRPC_OP_RECV_MESSAGE op.
{#fun unsafe create_receiving_byte_buffer as ^ {} -> `Ptr ByteBuffer' id#}

{#fun unsafe destroy_receiving_byte_buffer as ^ {id `Ptr ByteBuffer'} -> `()'#}

withByteBufferPtr :: (Ptr ByteBuffer -> IO a) -> IO a
withByteBufferPtr
  = bracket createReceivingByteBuffer destroyReceivingByteBuffer

-- | Takes an array of slices and the length of the array and returns a
-- 'ByteBuffer'.
{#fun grpc_raw_byte_buffer_create as ^ {`Slice', `CULong'} -> `ByteBuffer'#}

{#fun grpc_raw_compressed_byte_buffer_create as ^
  {`Slice', `CULong', `CompressionAlgorithm'} -> `ByteBuffer'#}

{#fun unsafe grpc_byte_buffer_copy as ^ {`ByteBuffer'} -> `ByteBuffer'#}

{#fun unsafe grpc_byte_buffer_length as ^ {`ByteBuffer'} -> `CULong'#}

{#fun unsafe grpc_byte_buffer_destroy as ^ {`ByteBuffer'} -> `()'#}

{#fun unsafe byte_buffer_reader_create as ^ {`ByteBuffer'} -> `ByteBufferReader'#}

{#fun unsafe byte_buffer_reader_destroy as ^ {`ByteBufferReader'} -> `()'#}

{#fun grpc_byte_buffer_reader_next as ^
  {`ByteBufferReader', `Slice'} -> `CInt'#}

-- | Returns a 'Slice' containing the entire contents of the 'ByteBuffer' being
-- read by the given 'ByteBufferReader'.
{#fun unsafe grpc_byte_buffer_reader_readall_ as ^ {`ByteBufferReader'} -> `Slice'#}

{#fun unsafe grpc_raw_byte_buffer_from_reader as ^
  {`ByteBufferReader'} -> `ByteBuffer'#}

withByteStringAsByteBuffer :: B.ByteString -> (ByteBuffer -> IO a) -> IO a
withByteStringAsByteBuffer bs f = do
  bracket (byteStringToSlice bs) freeSlice $ \slice -> do
    bracket (grpcRawByteBufferCreate slice 1) grpcByteBufferDestroy f

-- Creates a 'ByteBuffer'. We also return the slice we needed to allocate to
-- create it. It is the caller's responsibility to free both when finished using
-- the byte buffer. In most cases, one should prefer to use
-- 'withByteStringAsByteBuffer' if possible.
createByteBuffer :: B.ByteString -> IO (ByteBuffer, Slice)
createByteBuffer bs = do
  slice <- byteStringToSlice bs
  bb <- grpcRawByteBufferCreate slice 1
  return (bb, slice)

copyByteBufferToByteString :: ByteBuffer -> IO B.ByteString
copyByteBufferToByteString bb = do
  bracket (byteBufferReaderCreate bb) byteBufferReaderDestroy $ \bbr -> do
    bracket (grpcByteBufferReaderReadall bbr) freeSlice sliceToByteString
