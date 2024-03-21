-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Stream.StreamCore(
    Stream, newStream,
    Closed(..), onClose, closeStream,
    takeStream,
    writeStream,
    isClosed,
    ) where

import Control.Concurrent

newtype Stream a = Stream {status :: MVar (Either Closed (Open a))}

data Closed = EOS | Abnormal { reason :: String } deriving (Eq,Show)

-- TODO: better name than "clients" for the onClose callbacks?
data Open a = Open {
    chan :: Chan (Either Closed a),
    clients :: MVar [Closed -> IO ()] -- notified on close
    }

-- Create a new open stream.
newStream :: IO (Stream a)
newStream = do
    chan <- newChan
    clients <- newMVar []
    status <- newMVar (Right (Open {chan,clients}))
    return Stream{status}

-- Set a callback for when a stream becomes closed.
-- Called immediately if the stream is already closed.
onClose :: Stream a -> (Closed -> IO ()) -> IO ()
onClose Stream{status} f = do
    readMVar status >>= \case
        Left closed -> f closed -- already closed, call f now
        Right Open{clients} -> do
            modifyMVar_ clients (return . (f:))

-- Close stream now! Elements in flight are lost. Clients are notified.
closeStream :: Stream a -> Closed -> IO ()
closeStream Stream{status} closed = do
    takeMVar status >>= \case
        Left alreadyClosed -> putMVar status (Left alreadyClosed) -- do nothing
        Right Open{chan,clients} -> do
            putMVar status (Left closed) -- now can't get new clients
            fs <- readMVar clients
            mapM_ (\f -> f closed) fs
            -- forward the closure request in case of multiple blocked readers
            writeChan chan (Left closed)

-- Get the next element from a stream, or find the stream is closed.
-- (Blocking until one of the above occurs)
takeStream :: Stream a -> IO (Either Closed a)
takeStream Stream{status} = do
    readMVar status >>= \case
        Left closed -> return (Left closed)
        Right Open{chan} -> do
            readChan chan >>= \case
                Right a -> return (Right a)
                Left closed -> do
                    -- closed the stream, as requested by writer
                    modifyMVar_ status (\_ -> return (Left closed))
                    -- forward the closure request in case of multiple blocked readers
                    writeChan chan (Left closed)
                    return (Left closed)

-- Write an element onto the stream,
-- Or write a closure-request which happens after elems in flight are processed.
writeStream :: Stream a -> Either Closed a -> IO ()
writeStream Stream{status} item = do
    readMVar status >>= \case
        Left _closed -> return () -- already closed, item dropped on floor
        Right Open{chan} -> writeChan chan item

-- Is the stream closed right now?
isClosed :: Stream a -> IO (Maybe Closed)
isClosed Stream{status} =
    readMVar status >>= \case
        Left closed -> return (Just closed)
        Right _ -> return Nothing
