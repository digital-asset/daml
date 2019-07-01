-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- TODO: better name than "clients" for the onClose callbacks?

-- Streams which are closable at both ends.
-- When closed (at the read-end), elements in flight are dropped, clients are notified
-- When closure is requested at the write-end, elements in flight are processed. The stream becomes properly closed when the closure request reaches the read-end of the stream. Subsequent writes are dropped.

module DA.Ledger.Stream(
    Stream, newStream,
    Closed(..), onClose, closeStream,
    takeStream,
    writeStream,
    whenClosed, isClosed,
    mapListStream,
    streamToList,
    ) where

import Control.Concurrent

newtype Stream a = Stream {status :: MVar (Either Closed (Open a))}

data Closed = EOS | Abnormal { reason :: String } deriving Show

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

-- Block until the stream is closed, which might be already
whenClosed :: Stream a -> IO Closed
whenClosed stream = do
    signal <- newEmptyMVar
    onClose stream (putMVar signal)
    takeMVar signal

-- Is the stream closed right now?
isClosed :: Stream a -> IO (Maybe Closed)
isClosed Stream{status} =
    readMVar status >>= \case
        Left closed -> return (Just closed)
        Right _ -> return Nothing


-- Here a problem with Stream is revealed: To map one stream into another requires concurrency.
-- TODO: restructure processing to avoid the need for a sep Stream/PF
mapListStream :: (a -> IO [b]) -> Stream a -> IO (Stream b)
mapListStream f source = do
    target <- newStream
    onClose target (closeStream source)
    let loop = do
            takeStream source >>= \case
                Left closed -> writeStream target (Left closed)
                Right x -> do
                    ys <- f x
                    mapM_ (\y -> writeStream target (Right y)) ys
                    loop
    _ <- forkIO loop
    return target

-- Force a Stream, which we expect to reach EOS, to a list
streamToList :: Stream a -> IO [a]
streamToList stream = do
    takeStream stream >>= \case
        Left _ -> return []
        Right x -> fmap (x:) $ streamToList stream
