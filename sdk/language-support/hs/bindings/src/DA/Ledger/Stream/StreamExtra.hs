-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Stream.StreamExtra(
    mapListStream,
    streamToList,
    asyncStreamGen,
    ) where

import Control.Exception (SomeException,catch,mask_)
import Control.Concurrent.Async(async,cancel)
import DA.Ledger.Stream.StreamCore

-- To map one stream into another requires concurrency.
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
    _ <- async loop
    return target

-- Force a Stream, which we expect to reach EOS, to a list
-- fail if the stream is closed abnormally
streamToList :: Stream a -> IO [a]
streamToList stream = do
    takeStream stream >>= \case
        Left (Abnormal s) -> fail s
        Left EOS -> return []
        Right x -> fmap (x:) $ streamToList stream

-- Generate a stream in an asyncronous thread
asyncStreamGen :: (Stream a -> IO ()) -> IO (Stream a)
asyncStreamGen act = mask_ $ do
    stream <- newStream
    gen <- async $ act stream
        `catch` \(e::SomeException) -> closeStream stream (Abnormal (show e))
    onClose stream $ \_ -> cancel gen
    return stream
