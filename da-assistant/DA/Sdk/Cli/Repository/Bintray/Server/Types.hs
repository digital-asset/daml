-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude          #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}

module DA.Sdk.Cli.Repository.Bintray.Server.Types
    ( ServerConf(..)
    , ServerM(..)
    , ExternalFilePath(..)
    , runServerM
    , runServerM'
    , confFromBaseDir
    ) where

import           Control.Monad.Base            (MonadBase)
import           Control.Monad.Catch           hiding (Handler)
import           Control.Monad.Except          (MonadError(..))
import           Control.Monad.Logger          (LoggingT(..), MonadLogger, runFileLoggingT)
import           Control.Monad.Reader          (MonadIO, MonadReader, ReaderT(..))
import           Control.Monad.Trans.Control   (MonadBaseControl(..))
import           DA.Sdk.Prelude
import           DA.Sdk.Cli.Repository.Types   as Ty
import           Data.Text                     (Text)
import           Filesystem.Path               ((</>), FilePath)
import           Servant                       (Handler, runHandler, ServantErr(..))

data ServerConf = ServerConf { serverRootDir    :: Text
                             , serverSQLiteFile :: Text
                             , serverPkgsDir    :: Text
                             , serverLogPath    :: Maybe FilePath
                             }
  deriving
    ( Eq
    , Show
    )

-- | Creates the configuration by using the given path as server working directory
confFromBaseDir :: FilePath -> ServerConf
confFromBaseDir serverDir =
    ServerConf { serverRootDir    = pathToText serverDir
               , serverSQLiteFile = pathToText (serverDir </> textToPath "state.sqlite")
               , serverPkgsDir    = pathToText (serverDir </> textToPath "pkgsDir")
               , serverLogPath    = Nothing
               }

newtype ServerM a = ServerM { runServerMa :: LoggingT (ReaderT ServerConf Handler) a }
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadBase IO
    , MonadIO
    , MonadReader ServerConf
    , MonadLogger
    , MonadThrow
    , MonadCatch
    , MonadError ServantErr
    )

instance MonadBaseControl IO ServerM where
  type StM ServerM a = Either ServantErr a
  liftBaseWith f = ServerM $ liftBaseWith $ \q -> f (q . runServerMa)
  restoreM st = ServerM (restoreM st)

runServerM :: ServerConf -> ServerM a -> Handler a
runServerM serverConf serverM = do
    let loggingT = runServerMa serverM
        readerT  = maybe runNopLoggingT (runFileLoggingT . pathToString)
                         (serverLogPath serverConf) loggingT
    runReaderT readerT serverConf

runNopLoggingT :: LoggingT m a -> m a
runNopLoggingT = flip runLoggingT (\ _ _ _ _ -> pure ())

runServerM' :: ServerConf -> ServerM a -> IO (Either ServantErr a)
runServerM' c s = runHandler $ runServerM c s

-- A local filepath that is passed to the Bintray Mock to be used
-- as part of a version. The server will serve this local filepath
-- in the specific relative path.
data ExternalFilePath =
    ExternalFilePath
    { efpSubject        :: Ty.Subject
    , efpRepo           :: Ty.Repository
    , efpPackageName    :: Ty.PackageName
    , efpVersion        :: Ty.GenericVersion
    , efpServerFilePath :: Text
    , efpLocalFilePath  :: FilePath
    }
    deriving (Show)
