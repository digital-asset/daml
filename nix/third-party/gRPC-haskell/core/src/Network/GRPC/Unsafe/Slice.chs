{-# LANGUAGE StandaloneDeriving #-}

module Network.GRPC.Unsafe.Slice where

#include <grpc/slice.h>
#include <grpc_haskell.h>

import qualified Data.ByteString as B
import Foreign.C.String
import Foreign.C.Types
import Foreign.Ptr

-- | A 'Slice' is gRPC's string type. We can easily convert these to and from
-- ByteStrings. This type is a pointer to a C type.
{#pointer *grpc_slice as Slice newtype #}

deriving instance Show Slice

-- TODO: we could also represent this type as 'Ptr Slice', by doing this:
-- newtype Slice = Slice {#type grpc_slice#}
-- This would have no practical effect, but it would communicate intent more
-- clearly by emphasizing that the type is indeed a pointer and that the data
-- it is pointing to might change/be destroyed after running IO actions. To make
-- the change, we would just need to change all occurrences of 'Slice' to
-- 'Ptr Slice' and add 'castPtr' in and out marshallers.
-- This seems like the right way to do it, but c2hs doesn't make it easy, so
-- maybe the established idiom is to do what c2hs does.

-- | Get the length of a slice.
{#fun unsafe grpc_slice_length_ as ^ {`Slice'} -> `CULong'#}

-- | Slices allocated using this function must be freed by
-- 'freeSlice'
{#fun unsafe grpc_slice_malloc_ as ^ {`Int'} -> `Slice'#}

-- | Returns a pointer to the start of the character array contained by the
-- slice.
{#fun unsafe grpc_slice_start_ as ^ {`Slice'} -> `Ptr CChar' castPtr #}

-- | Slices must be freed using 'freeSlice'.
{#fun unsafe grpc_slice_from_copied_buffer_ as ^ {`CString', `Int'} -> `Slice'#}

-- | Properly cleans up all memory used by a 'Slice'. Danger: the Slice should
-- not be used after this function is called on it.
{#fun unsafe free_slice as ^ {`Slice'} -> `()'#}

-- | Copies a 'Slice' to a ByteString.
-- TODO: there are also non-copying unsafe ByteString construction functions.
-- We could gain some speed by using them.
-- idea would be something :: (ByteString -> Response) -> IO () that handles
-- getting and freeing the slice behind the scenes.
sliceToByteString :: Slice -> IO B.ByteString
sliceToByteString slice = do
  len <- fmap fromIntegral $ grpcSliceLength slice
  str <- grpcSliceStart slice
  B.packCStringLen (str, len)

-- | Copies a 'ByteString' to a 'Slice'.
byteStringToSlice :: B.ByteString -> IO Slice
byteStringToSlice bs = B.useAsCStringLen bs $ uncurry grpcSliceFromCopiedBuffer
