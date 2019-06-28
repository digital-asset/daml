-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
-- | This is a compatibility module that abstracts over the
-- concrete choice of logging framework so users can plug in whatever
-- framework they want to.
module Development.IDE.Types.Logger
  ( Logger(..)
  , makeOneLogger
  , makeNopLogger
  ) where

import qualified Data.Text as T

-- | Note that this is logging actions _of the program_, not of the user.
--   You shouldn't call warning/error if the user has caused an error, only
--   if our code has gone wrong and is itself erroneous (e.g. we threw an exception).
data Logger = Logger {
      logError :: T.Text -> IO ()
    , logInfo :: T.Text -> IO ()
    , logDebug :: T.Text -> IO ()
    , logWarning :: T.Text -> IO ()
    }

makeNopLogger :: Logger
makeNopLogger = makeOneLogger $ const $ pure ()

makeOneLogger :: (T.Text -> IO ()) -> Logger
makeOneLogger x = Logger x x x x
