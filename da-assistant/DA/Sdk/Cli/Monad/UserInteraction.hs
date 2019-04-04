-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DefaultSignatures #-}

module DA.Sdk.Cli.Monad.UserInteraction
    ( MonadUserInput (..)
    , readPropMb
    , MonadUserOutput (..)
    , renderPretty
    , ReadInputLineFailed (..)
    ) where

import qualified DA.Sdk.Cli.Conf.Types as Conf
import qualified DA.Sdk.Cli.Conf.NameToProp as Conf
import           DA.Sdk.Prelude
import qualified Control.Monad.Logger as L
import           Data.Text (pack)
import qualified DA.Sdk.Pretty as P
import Control.Monad.Trans.Except
import Control.Monad.Trans.Class (lift)
import DA.Sdk.Cli.Monad
import qualified DA.Sdk.Cli.System as Sys
import DA.Sdk.Control.Exception.Extra

data ReadInputLineFailed = ReadInputLineFailed IOError deriving (Eq, Show)

class Monad m => MonadUserInput m where
    readProp :: Conf.Name -> Text -> m (Either Text Conf.Prop)
    readInputLine :: m (Either ReadInputLineFailed Text)

    default readProp :: MonadIO m => Conf.Name -> Text -> m (Either Text Conf.Prop)
    readProp name prompt = liftIO $ do
        mbText <- readLine prompt
        return $ maybe (Left "Only EOF was read.")
                       (Conf.nameValToProp name)
                       mbText
    
    default readInputLine :: MonadIO m => m (Either ReadInputLineFailed Text)
    readInputLine = liftIO $ tryWith ReadInputLineFailed $ liftIO $ pack <$> getLine

instance MonadUserInput IO
instance MonadUserInput (L.LoggingT IO)
instance MonadUserInput CliM

readPropMb :: MonadUserInput m => Conf.Name -> Text -> m (Maybe Conf.Prop)
readPropMb name t = either (const Nothing) Just <$> readProp name t

class Monad m => MonadUserOutput m where
    displayNoLine :: Text -> m ()
    display :: Text -> m ()
    displayStr :: String -> m ()
    displayStr = display . pack
    displayDoc :: P.Doc a -> m ()
    displayDoc d = renderPretty d >>= display
    displayPretty :: P.Pretty p => p -> m ()
    displayPretty = displayDoc . P.pretty
    displayNewLine :: m ()
    displayNewLine = display ""
    getTermWidth :: m (Maybe Int)

    default display :: MonadIO m => Text -> m ()
    display = liftIO . putTextLn

    default displayNoLine :: MonadIO m => Text -> m ()
    displayNoLine = liftIO . putText

instance MonadUserOutput IO where
    getTermWidth = Sys.getTermWidth
instance MonadUserOutput (L.LoggingT IO) where
    getTermWidth = liftIO Sys.getTermWidth
instance MonadUserOutput m => MonadUserOutput (ExceptT e m) where
    display = lift . display
    getTermWidth = lift getTermWidth
    displayNoLine = lift . displayNoLine
instance MonadUserOutput CliM where
    getTermWidth = Just <$> getTermWidthConfig

--------------------------------------------------------------------------------
-- Pretty Printing
--------------------------------------------------------------------------------

renderPretty :: MonadUserOutput m => P.Doc ann -> m Text
renderPretty doc = do
    termWidthMb <- getTermWidth
    case termWidthMb of
      Just termWidth ->
        pure $ P.renderPlainWidth termWidth doc
      Nothing ->
        pure $ P.renderPlain doc