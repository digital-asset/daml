{-# LANGUAGE ForeignFunctionInterface #-}
module Lib where

foreign import ccall "grpc_completion_queue_next42"
    sendMessageTimeout :: IO ()
