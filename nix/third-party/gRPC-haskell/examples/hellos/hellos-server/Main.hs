{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-unused-binds       #-}

import           Control.Monad
import           Data.Function                              (fix)
import qualified Data.Text                                  as T
import           Data.Word
import           GHC.Generics                               (Generic)
import           Network.GRPC.HighLevel.Server
import qualified Network.GRPC.HighLevel.Server.Unregistered as U
import           Network.GRPC.LowLevel
import           Proto3.Suite.Class

serverMeta :: MetadataMap
serverMeta = [("test_meta", "test_meta_value")]

data SSRqt = SSRqt { ssName :: T.Text, ssNumReplies :: Word32 } deriving (Show, Eq, Ord, Generic)
instance Message SSRqt
data SSRpy = SSRpy { ssGreeting :: T.Text } deriving (Show, Eq, Ord, Generic)
instance Message SSRpy
data CSRqt = CSRqt { csMessage :: T.Text } deriving (Show, Eq, Ord, Generic)
instance Message CSRqt
data CSRpy = CSRpy { csNumRequests :: Word32 } deriving (Show, Eq, Ord, Generic)
instance Message CSRpy
data BiRqtRpy = BiRqtRpy { biMessage :: T.Text } deriving (Show, Eq, Ord, Generic)
instance Message BiRqtRpy

expect :: (Eq a, Monad m, Show a) => String -> a -> a -> m ()
expect ctx ex got
  | ex /= got = fail $ ctx ++ " error: expected " ++ show ex ++ ", got " ++ show got
  | otherwise = return ()

helloSS :: Handler 'ServerStreaming
helloSS = ServerStreamHandler "/hellos.Hellos/HelloSS" $ \sc send -> do
  let SSRqt{..} = payload sc
  replicateM_ (fromIntegral ssNumReplies) $ do
    eea <- send $ SSRpy $ "Hello there, " <> ssName <> "!"
    case eea of
      Left e  -> fail $ "helloSS error: " ++ show e
      Right{} -> return ()
  return (serverMeta, StatusOk, StatusDetails "helloSS response details")

helloCS :: Handler 'ClientStreaming
helloCS = ClientStreamHandler "/hellos.Hellos/HelloCS" $ \_ recv -> flip fix 0 $ \go n ->
  recv >>= \case
    Left e           -> fail $ "helloCS error: " ++ show e
    Right Nothing    -> return (Just (CSRpy n), mempty, StatusOk, StatusDetails "helloCS details")
    Right (Just rqt) -> do
      expect "helloCS" "client streaming payload" (csMessage rqt)
      go (n+1)

helloBi :: Handler 'BiDiStreaming
helloBi = BiDiStreamHandler "/hellos.Hellos/HelloBi" $ \_ recv send -> fix $ \go ->
  recv >>= \case
    Left e           -> fail $ "helloBi recv error: " ++ show e
    Right Nothing    -> return (mempty, StatusOk, StatusDetails "helloBi details")
    Right (Just rqt) -> do
      expect "helloBi" "bidi payload" (biMessage rqt)
      send rqt >>= \case
        Left e -> fail $ "helloBi send error: " ++ show e
        _      -> go

highlevelMainUnregistered :: IO ()
highlevelMainUnregistered =
  U.serverLoop defaultOptions{
      optServerStreamHandlers = [helloSS]
    , optClientStreamHandlers = [helloCS]
    , optBiDiStreamHandlers   = [helloBi]
  }

main :: IO ()
main = highlevelMainUnregistered

defConfig :: ServerConfig
defConfig = ServerConfig "localhost" 50051 [] [] [] [] [] Nothing
