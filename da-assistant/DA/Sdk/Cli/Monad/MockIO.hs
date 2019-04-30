-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE InstanceSigs   #-}
{-# LANGUAGE OverloadedStrings   #-}

module DA.Sdk.Cli.Monad.MockIO(module DA.Sdk.Cli.Monad.MockIO) where

import DA.Sdk.Prelude
import Control.Monad.Trans.Except
import qualified Control.Monad.Logger as L
import DA.Sdk.Cli.Conf.Types (Prop(..), Props(..), Project(..), ProjectMessage)
import DA.Sdk.Version
import Control.Monad.Trans.Class (lift)
import Control.Monad.Reader
import qualified DA.Sdk.Cli.Locations.Types as L
import Data.Time.Clock.POSIX
import qualified Turtle as T
import qualified Data.Time as T

class Monad m => MockIO m where
    mockIO :: MockAction t -> IO t -> m t

data MockAction t where
    ConstMock :: t -> MockAction t

deriving instance Show t => Show (MockAction t)

mockIO_ :: MockIO m => IO () -> m ()
mockIO_ = mockIO (ConstMock ())

mockGetProps :: MockIO m => IO [Prop] -> m [Prop]
mockGetProps = mockIO (ConstMock []) -- TODO add something meaningful

mockGetErrOrProps :: (MockIO m, Show e) => IO (Either e Props) -> m (Either e Props)
mockGetErrOrProps = mockIO (ConstMock $ Right $ Props [])

mockTestFileExists :: MockIO m => IO Bool -> m Bool
mockTestFileExists = mockIO (ConstMock True)

mockHomeDirPath :: MockIO m => IO (L.FilePathOf f) -> m (L.FilePathOf f)
mockHomeDirPath = mockIO (ConstMock $ L.FilePathOf "~/.da")

mockBackupConfFile :: MockIO m => IO FilePath -> m FilePath
mockBackupConfFile = mockIO (ConstMock "filepath")

mockProjectMasterFile :: MockIO m => IO (Maybe FilePath) -> m (Maybe FilePath)
mockProjectMasterFile = mockIO (ConstMock $ Just "filepath")

mockDirectoryContents :: MockIO m => IO [String] -> m [String]
mockDirectoryContents = mockIO (ConstMock ["filepath"])

mockDirectoryContents' :: MockIO m => IO [FilePath] -> m [FilePath]
mockDirectoryContents' = mockIO (ConstMock ["filepath"])

mockGetProject :: MockIO m => IO (Either ProjectMessage Project) -> m (Either ProjectMessage Project)
mockGetProject = mockIO (ConstMock $ Right proj)
  where
    proj = Project "path" "name" (SemVersion 1 2 3 Nothing) "source" Nothing ["party1"] []

mockGetTime :: MockIO m => IO POSIXTime -> m POSIXTime
mockGetTime = mockIO (ConstMock posixDayLength)

mockGetTime' :: MockIO m => IO T.UTCTime -> m T.UTCTime
mockGetTime' = mockIO (ConstMock $ T.UTCTime (T.fromGregorian 1999 10 10) 0)

mockGetArgs :: MockIO m => IO [Text] -> m [Text]
mockGetArgs = mockIO (ConstMock [])

mockWhich :: MockIO m => IO (Maybe FilePath) -> m (Maybe FilePath)
mockWhich = mockIO (ConstMock $ Just "/some/bin")

instance MockIO m => MockIO (ExceptT e m) where
    mockIO :: MockAction t -> IO t -> ExceptT e m t -- IO (Either ...) ---> ExceptT e IO (Either ...) is that OK?? (GH)
    mockIO a = lift . mockIO a

instance MockIO IO where
    mockIO _a ioOp = ioOp

instance MockIO (L.LoggingT IO) where
    mockIO _a ioOp = liftIO ioOp

instance MockIO m => MockIO (ReaderT e m) where
    mockIO a = lift . mockIO a