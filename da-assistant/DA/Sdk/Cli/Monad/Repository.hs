-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances   #-}

module DA.Sdk.Cli.Monad.Repository
    ( MonadRepo (..)
    , makeHandle
    ) where

import qualified DA.Sdk.Cli.Repository.Types as Repo
import qualified DA.Sdk.Cli.Conf.Types as Conf
import qualified Control.Monad.Logger as L
import qualified DA.Sdk.Cli.Repository as R
import qualified DA.Sdk.Cli.Monad as M

class Monad m => MonadRepo m where
    makeHandleWithCredentials :: Conf.RepositoryURLs -> R.Credentials -> m (Repo.Handle m)

instance MonadRepo (L.LoggingT IO) where
    makeHandleWithCredentials = R.makeHandleWithCredentials

instance MonadRepo M.CliM where
    makeHandleWithCredentials = R.makeHandleWithCredentials

makeHandle :: MonadRepo m => Conf.Conf -> m (Repo.Handle m)
makeHandle conf = makeHandleWithCredentials repository credentials
    where
        credentials = Conf.confCredentials conf
        repository  = Conf.confRepositoryURLs conf