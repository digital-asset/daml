{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module Main where

import Prelude hiding (sum)

import Simple

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.IO.Class

import Data.Monoid
import Data.Foldable (sum)
import Data.String

import Network.GRPC.LowLevel
import Network.GRPC.HighLevel.Server
import Network.GRPC.HighLevel.Generated (defaultServiceOptions)

handleNormalCall :: ServerRequest 'Normal SimpleServiceRequest SimpleServiceResponse -> IO (ServerResponse 'Normal SimpleServiceResponse)
handleNormalCall (ServerNormalRequest meta (SimpleServiceRequest request nums)) =
  pure (ServerNormalResponse (SimpleServiceResponse request result) mempty StatusOk (StatusDetails ""))
  where result = sum nums

handleClientStreamingCall :: ServerRequest 'ClientStreaming SimpleServiceRequest SimpleServiceResponse -> IO (ServerResponse 'ClientStreaming SimpleServiceResponse)
handleClientStreamingCall (ServerReaderRequest call recvRequest) = go 0 ""
  where go sumAccum nameAccum =
          recvRequest >>= \req ->
          case req of
            Left ioError -> pure (ServerReaderResponse Nothing mempty StatusCancelled (StatusDetails ("handleClientStreamingCall: IO error: " <> fromString (show ioError))))
            Right Nothing ->
              pure (ServerReaderResponse (Just (SimpleServiceResponse nameAccum sumAccum)) mempty StatusOk (StatusDetails ""))
            Right (Just (SimpleServiceRequest name nums)) ->
              go (sumAccum + sum nums) (nameAccum <> name)

handleServerStreamingCall :: ServerRequest 'ServerStreaming SimpleServiceRequest SimpleServiceResponse -> IO (ServerResponse 'ServerStreaming SimpleServiceResponse)
handleServerStreamingCall (ServerWriterRequest call (SimpleServiceRequest requestName nums) sendResponse) = go
  where go = do forM_ nums $ \num ->
                  sendResponse (SimpleServiceResponse requestName num)
                pure (ServerWriterResponse mempty StatusOk (StatusDetails ""))

handleBiDiStreamingCall :: ServerRequest 'BiDiStreaming SimpleServiceRequest SimpleServiceResponse -> IO (ServerResponse 'BiDiStreaming SimpleServiceResponse)
handleBiDiStreamingCall (ServerBiDiRequest call recvRequest sendResponse) = go
  where go = recvRequest >>= \req ->
             case req of
               Left ioError ->
                 pure (ServerBiDiResponse mempty StatusCancelled (StatusDetails ("handleBiDiStreamingCall: IO error: " <> fromString (show ioError))))
               Right Nothing ->
                 pure (ServerBiDiResponse mempty StatusOk (StatusDetails ""))
               Right (Just (SimpleServiceRequest name nums)) ->
                 do sendResponse (SimpleServiceResponse name (sum nums))
                    go

handleDone :: MVar () -> ServerRequest 'Normal SimpleServiceDone SimpleServiceDone -> IO (ServerResponse 'Normal SimpleServiceDone)
handleDone exitVar (ServerNormalRequest _ req) =
  do forkIO (threadDelay 5000 >> putMVar exitVar ())
     pure (ServerNormalResponse req mempty StatusOk (StatusDetails ""))

main :: IO ()
main = do exitVar <- newEmptyMVar

          forkIO $ simpleServiceServer (SimpleService
            { simpleServiceDone = handleDone exitVar
            , simpleServiceNormalCall = handleNormalCall
            , simpleServiceClientStreamingCall = handleClientStreamingCall
            , simpleServiceServerStreamingCall = handleServerStreamingCall
            , simpleServiceBiDiStreamingCall = handleBiDiStreamingCall })
            defaultServiceOptions

          takeMVar exitVar
