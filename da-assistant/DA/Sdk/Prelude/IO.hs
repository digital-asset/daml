-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}

module DA.Sdk.Prelude.IO
    ( putText
    , putTextLn
    , readLine
    ) where

import           Control.Monad.IO.Class
import qualified Data.Text                as T
import qualified Data.Text.IO             as T.IO
import           Prelude                  ((.), Maybe, fmap, (<$>), Bool(..))
import qualified System.Console.Haskeline as Haskeline

-- | Output a text to stdout.
putText :: MonadIO m => T.Text -> m ()
putText = liftIO . T.IO.putStr

-- | Output a text to stdout. Appends a new line at the end.
putTextLn :: MonadIO m => T.Text -> m ()
putTextLn = liftIO . T.IO.putStrLn

-- | Read a single line of user input from stdin.
readLine :: Haskeline.MonadException m => T.Text -> m (Maybe T.Text)
readLine promptT =
    fmap T.pack <$> liftIO (Haskeline.runInputT options (Haskeline.getInputLine promptS))
  where
    promptS = T.unpack promptT
    options = Haskeline.defaultSettings { Haskeline.autoAddHistory = False }
