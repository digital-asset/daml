{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception                          (bracket)
import           Control.Monad
import           Criterion.Main
import           Criterion.Types                            (Config (..))
import qualified Data.ByteString.Lazy                       as BL
import           Data.Word
import           GHC.Generics                               (Generic)
import           Network.GRPC.HighLevel.Server              hiding (serverLoop)
import           Network.GRPC.HighLevel.Server.Unregistered (serverLoop)
import           Network.GRPC.LowLevel
import           Network.GRPC.LowLevel.GRPC                 (threadDelaySecs)
import           Proto3.Suite.Class
import           Proto3.Suite.Types
import           System.Random                              (randomRIO)

data AddRequest = AddRequest {addX   :: Fixed Word32
                              , addY :: Fixed Word32}
  deriving (Show, Eq, Ord, Generic)
instance Message AddRequest

data AddResponse = AddResponse (Fixed Word32)
  deriving (Show, Eq, Ord, Generic)
instance Message AddResponse

addMethod :: MethodName
addMethod = MethodName "/unary"

addClientStreamMethod :: MethodName
addClientStreamMethod = MethodName "/clientstream"

addServerStreamMethod :: MethodName
addServerStreamMethod = MethodName "/serverstream"

addBiDiMethod :: MethodName
addBiDiMethod = MethodName "/bidistream"

addHandler :: Handler 'Normal
addHandler =
  UnaryHandler addMethod $
    \c -> do
      let b = payload c
      return ( AddResponse $ addX b + addY b
             , metadata c
             , StatusOk
             , StatusDetails ""
             )

addClientStreamHandler :: Handler 'ClientStreaming
addClientStreamHandler =
  ClientStreamHandler addClientStreamMethod $
  \_ recv -> do
    answer <- go recv 0
    return (Just answer, mempty, StatusOk, "")
    where go recv !i = do
            req <- recv
            case req of
              Left _ -> return $ AddResponse i
              Right Nothing -> return $ AddResponse i
              Right (Just (AddRequest x y)) -> go recv (i+x+y)

addServerStreamHandler :: Handler 'ServerStreaming
addServerStreamHandler =
  ServerStreamHandler addServerStreamMethod $
  \c send -> do
    let AddRequest (Fixed x) y = payload c
    replicateM_ (fromIntegral x) $ send $ AddResponse y
    return (mempty, StatusOk, "")

addBiDiHandler :: Handler 'BiDiStreaming
addBiDiHandler = BiDiStreamHandler addBiDiMethod (go 0)
  where go :: Fixed Word32 -> ServerRWHandler AddRequest AddResponse
        go !i c recv send = do
          req <- recv
          case req of
            Left _ -> return (mempty, StatusOk, "")
            Right Nothing -> return (mempty, StatusOk, "")
            Right (Just (AddRequest x y)) -> do
              let curr = i + x + y
              void $ send $ AddResponse curr
              go curr c recv send


serverOpts :: ServerOptions
serverOpts =
  defaultOptions{optNormalHandlers         = [addHandler]
                 , optClientStreamHandlers = [addClientStreamHandler]
                 , optServerStreamHandlers = [addServerStreamHandler]
                 , optBiDiStreamHandlers   = [addBiDiHandler]}

main :: IO ()
main = bracket startServer stopServer $ const $ withGRPC $ \grpc ->
  withClient grpc (ClientConfig "localhost" 50051 [] Nothing) $ \c -> do
    rmAdd <- clientRegisterMethodNormal c addMethod
    rmClientStream <- clientRegisterMethodClientStreaming c addClientStreamMethod
    rmServerStream <- clientRegisterMethodServerStreaming c addServerStreamMethod
    rmBiDiStream <- clientRegisterMethodBiDiStreaming c addBiDiMethod
    defaultMainWith
      defaultConfig{reportFile = Just "benchmarks.html"}
      [ bench "unary request" $ nfIO (addRequest c rmAdd)
      , bench "client stream: 100 messages" $ nfIO (addClientStream c rmClientStream 100)
      , bench "client stream: 1k messages" $ nfIO (addClientStream c rmClientStream 1000)
      , bench "client stream: 10k messages" $ nfIO (addClientStream c rmClientStream 10000)
      , bench "server stream: 100 messages" $ nfIO (addServerStream c rmServerStream 100)
      , bench "server stream: 1k messages" $ nfIO (addServerStream c rmServerStream 1000)
      , bench "server stream: 10k messages" $ nfIO (addServerStream c rmServerStream 10000)
      , bench "bidi stream: 50 messages up, 50 down" $ nfIO (bidiStream c rmBiDiStream 50)
      , bench "bidi stream: 500 message up, 500 down" $ nfIO (bidiStream c rmBiDiStream 500)
      , bench "bidi stream: 5000 messages up, 5000 down" $ nfIO (bidiStream c rmBiDiStream 5000)]

  where startServer = do
          sThrd <- async $ serverLoop serverOpts
          threadDelaySecs 1
          return sThrd

        stopServer sThrd = cancel sThrd >> void (waitCatch sThrd)

        encode = BL.toStrict . toLazyByteString

        addRequest c rmAdd = do
          x <- fmap Fixed $ randomRIO (0,1000)
          y <- fmap Fixed $ randomRIO (0,1000)
          let addEnc = BL.toStrict . toLazyByteString $ AddRequest x y
          clientRequest c rmAdd 5 addEnc mempty >>= \case
            Left e -> fail $ "Got client error on add request: " ++ show e
            Right r -> case fromByteString (rspBody r) of
              Left e -> fail $ "failed to decode add response: " ++ show e
              Right dec
                | dec == AddResponse (x + y) -> return ()
                | otherwise -> fail $ "Got wrong add answer: " ++ show dec ++ "; expected: " ++ show x ++ " + " ++ show y ++ " = " ++ show (x+y)

        addClientStream c rm i = do
          let msg = encode $ AddRequest 1 0
          Right (Just r,_,_,_,_) <- clientWriter c rm 5 mempty $ \send -> do
            replicateM_ i $ send msg
          let decoded = fromByteString r
          when (decoded /= Right (AddResponse (fromIntegral i))) $
            fail $ "clientStream: bad answer: " ++ show decoded ++ "; expected: " ++ show i

        addServerStream c rm i = do
          let msg = encode $ AddRequest (fromIntegral i) 2
          Right (_, _, sd) <- clientReader c rm 5 msg mempty $ \_ recv ->
            replicateM_ i $ do
              Right (Just bs) <- recv
              let Right decoded = fromByteString bs
              when (decoded /= AddResponse 2) $
                fail $ "serverStream: bad response of " ++ show decoded ++ "; expected 2."
          when (sd /= mempty) $ fail $ "bad status details: " ++ show sd

        bidiStream c rm i = do
          Right (_, _, sd) <- clientRW c rm 5 mempty $ \_ recv send done -> do
            forM_ (take i [2,4..]) $ \n -> do
              void $ send $ encode $ AddRequest 1 1
              Right (Just bs) <- recv
              let Right decoded = fromByteString bs
              when (decoded /= AddResponse n) $
                fail $ "bidiStream: got: " ++ show decoded ++ "expected: " ++ show n
            void done
          when (sd /= mempty) $ fail $ "bad StatusDetails: " ++ show sd
