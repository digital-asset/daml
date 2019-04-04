{-# LANGUAGE RecordWildCards #-}

module Network.GRPC.Unsafe.ChannelArgs where

import           Control.Exception
import           Control.Monad
import           Foreign.Marshal.Alloc (malloc, free)
import           Foreign.Storable
import           Numeric.Natural

#include <grpc/grpc.h>
#include <grpc/status.h>
#include <grpc/support/alloc.h>
#include <grpc/impl/codegen/compression_types.h>
#include <grpc_haskell.h>

{#enum supported_arg_key as ArgKey {underscoreToCase} deriving (Show, Eq)#}

{#enum grpc_compression_algorithm
  as CompressionAlgorithm {underscoreToCase} deriving (Show, Eq)#}

{#enum grpc_compression_level
  as CompressionLevel {underscoreToCase} deriving (Show, Eq)#}

{#pointer *grpc_arg as ^#}

data ChannelArgs = ChannelArgs {channelArgsN :: Int,
                                channelArgsArray :: GrpcArg}
  deriving (Show, Eq)

{#pointer *grpc_channel_args as ^ -> ChannelArgs#}

instance Storable ChannelArgs where
  sizeOf _ = {#sizeof grpc_channel_args#}
  alignment _ = {#alignof grpc_channel_args#}
  peek p = ChannelArgs <$> fmap fromIntegral
                                ({#get grpc_channel_args->num_args#} p)
                       <*> ({#get grpc_channel_args->args#} p)
  poke p ChannelArgs{..} = do
    {#set grpc_channel_args.num_args#} p $ fromIntegral channelArgsN
    {#set grpc_channel_args.args#} p channelArgsArray

{#fun create_arg_array as ^ {`Int'} -> `GrpcArg'#}

data ArgValue = StringArg String | IntArg Int
  deriving (Show, Eq)

-- | Supported arguments for a channel. More cases will be added as we figure
-- out what they are.
data Arg = CompressionAlgArg CompressionAlgorithm
         | CompressionLevelArg CompressionLevel
         | UserAgentPrefix String
         | UserAgentSuffix String
         | MaxReceiveMessageLength Natural
         | MaxMetadataSize Natural
  deriving (Show, Eq)

{#fun create_string_arg as ^ {`GrpcArg', `Int', `ArgKey', `String'} -> `()'#}

{#fun create_int_arg as ^ {`GrpcArg', `Int', `ArgKey', `Int'} -> `()'#}

{#fun destroy_arg_array as ^ {`GrpcArg', `Int'} -> `()'#}

createArg :: GrpcArg -> Arg -> Int -> IO ()
createArg array (CompressionAlgArg alg) i =
  createIntArg array i CompressionAlgorithmKey (fromEnum alg)
createArg array (CompressionLevelArg lvl) i =
  createIntArg array i CompressionLevelKey (fromEnum lvl)
createArg array (UserAgentPrefix prefix) i =
  createStringArg array i UserAgentPrefixKey prefix
createArg array (UserAgentSuffix suffix) i =
  createStringArg array i UserAgentSuffixKey suffix
createArg array (MaxReceiveMessageLength n) i =
  createIntArg array i MaxReceiveMessageLengthKey $
    fromIntegral (min n (fromIntegral (maxBound :: Int)))
createArg array (MaxMetadataSize n) i =
  createIntArg array i MaxMetadataSizeKey $
    fromIntegral (min n (fromIntegral (maxBound :: Int)))

createChannelArgs :: [Arg] -> IO GrpcChannelArgs
createChannelArgs args = do
  let l = length args
  array <- createArgArray l
  forM_ (zip [0..l] args) $ \(i, arg) -> createArg array arg i
  ptr <- malloc
  poke ptr $ ChannelArgs l array
  return ptr

destroyChannelArgs :: GrpcChannelArgs -> IO ()
destroyChannelArgs ptr =
  do ChannelArgs{..} <- peek ptr
     destroyArgArray channelArgsArray channelArgsN
     free ptr

withChannelArgs :: [Arg] -> (GrpcChannelArgs -> IO a) -> IO a
withChannelArgs args f = bracket (createChannelArgs args) destroyChannelArgs f
