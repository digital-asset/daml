{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns        #-}
{-# OPTIONS_GHC -fno-warn-unused-binds       #-}

import           Control.Concurrent.Async
import           Control.Monad
import qualified Data.ByteString.Lazy                      as BL
import           Data.Function
import qualified Data.Text                                 as T
import           Data.Word
import           GHC.Generics                              (Generic)
import           Network.GRPC.LowLevel
import           Proto3.Suite.Class

helloSS, helloCS, helloBi :: MethodName
helloSS = MethodName "/hellos.Hellos/HelloSS"
helloCS = MethodName "/hellos.Hellos/HelloCS"
helloBi = MethodName "/hellos.Hellos/HelloBi"

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

doHelloSS :: Client -> Int -> IO ()
doHelloSS c n = do
  rm <- clientRegisterMethodServerStreaming c helloSS
  let pay        = SSRqt "server streaming mode" (fromIntegral n)
      enc        = BL.toStrict . toLazyByteString $ pay
      err desc e = fail $ "doHelloSS: " ++ desc ++ " error: " ++ show e
  eea <- clientReader c rm n enc mempty $ \_md recv -> do
    n' <- flip fix (0::Int) $ \go i -> recv >>= \case
      Left e          -> err "recv" e
      Right Nothing   -> return i
      Right (Just bs) -> case fromByteString bs of
        Left e  -> err "decoding" e
        Right r -> expect "doHelloSS/rpy" expay (ssGreeting r) >> go (i+1)
    expect "doHelloSS/cnt" n n'
  case eea of
    Left e             -> err "clientReader" e
    Right (_, st, _)
      | st /= StatusOk -> fail "clientReader: non-OK status"
      | otherwise      -> putStrLn "doHelloSS: RPC successful"
  where
    expay     = "Hello there, server streaming mode!"

doHelloCS :: Client -> Int -> IO ()
doHelloCS c n = do
  rm  <- clientRegisterMethodClientStreaming c helloCS
  let pay = CSRqt "client streaming payload"
      enc = BL.toStrict . toLazyByteString $ pay
  eea <- clientWriter c rm n mempty $ \send ->
    replicateM_ n $ send enc >>= \case
      Left e  -> fail $ "doHelloCS: send error: " ++ show e
      Right{} -> return ()
  case eea of
    Left e                      -> fail $ "clientWriter error: " ++ show e
    Right (Nothing, _, _, _, _) -> fail "clientWriter error: no reply payload"
    Right (Just bs, _init, _trail, st, _dtls)
      | st /= StatusOk -> fail "clientWriter: non-OK status"
      | otherwise -> case fromByteString bs of
          Left e    -> fail $ "Decoding error: " ++ show e
          Right dec -> do
            expect "doHelloCS/cnt" (fromIntegral n) (csNumRequests dec)
            putStrLn "doHelloCS: RPC successful"

doHelloBi :: Client -> Int -> IO ()
doHelloBi c n = do
  rm <- clientRegisterMethodBiDiStreaming c helloBi
  let pay        = BiRqtRpy "bidi payload"
      enc        = BL.toStrict . toLazyByteString $ pay
      err desc e = fail $ "doHelloBi: " ++ desc ++ " error: " ++ show e
  eea <- clientRW c rm n mempty $ \_getMD recv send writesDone -> do
    -- perform n writes on a worker thread
    thd <- async $ do
      replicateM_ n $ send enc >>= \case
        Left e -> err "send" e
        _      -> return ()
      writesDone >>= \case
        Left e -> err "writesDone" e
        _      -> return ()
    -- perform reads on this thread until the stream is terminated
    -- emd <- getMD; putStrLn ("getMD result: " ++ show emd)
    fix $ \go -> recv >>= \case
      Left e          -> err "recv" e
      Right Nothing   -> return ()
      Right (Just bs) -> case fromByteString bs of
        Left e  -> err "decoding" e
        Right r -> when (r /= pay) (fail "Reply payload mismatch") >> go
    wait thd
  case eea of
    Left e           -> err "clientRW'" e
    Right (_, st, _) -> do
      when (st /= StatusOk) $ fail $ "clientRW: non-OK status: " ++ show st
      putStrLn "doHelloBi: RPC successful"

highlevelMain :: IO ()
highlevelMain = withGRPC $ \g ->
  withClient g (ClientConfig "localhost" 50051 [] Nothing) $ \c -> do
    let n = 100000
    putStrLn "-------------- HelloSS --------------"
    doHelloSS c n
    putStrLn "-------------- HelloCS --------------"
    doHelloCS c n
    putStrLn "-------------- HelloBi --------------"
    doHelloBi c n

main :: IO ()
main = highlevelMain
