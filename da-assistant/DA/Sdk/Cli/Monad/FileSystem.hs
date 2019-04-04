-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}

module DA.Sdk.Cli.Monad.FileSystem
    ( MonadFS (..)
    , MkTreeFailed (..)
    , CpTreeFailed (..)
    , CpFailed (..)
    , LsFailed (..)
    , DateFileFailed (..)
    , WithTempDirectoryFailed (..)
    , WithSysTempDirectoryFailed (..)
    , DecodeYamlFileFailed (..)
    , EncodeYamlFileFailed (..)
    , UnTarGzipFailed (..)
    , PwdFailed (..)
    , SetFileModeFailed (..)
    , WriteFileFailed (..)
    , TouchFailed (..)
    , RemoveFailed (..)
    , BinaryStarterCreationFailed (..)
    , ReadFileFailed (..)
    , ReadFileUtf8Failed (..)
    , testdir
    , testfile
    , mktree
    , cptree
    , cp
    , ls
    , datefile
    , withTempDirectory
    , decodeYamlFile
    , encodeYamlFile
    , writeFile
    , rm
    , readFileUtf8
    , createBinaryStarter
    ) where


import DA.Sdk.Prelude hiding (writeFile)
import DA.Sdk.Cli.Locations.Types
import DA.Sdk.Shell.Tar as G
import qualified Turtle as T
import qualified System.IO.Temp as Tmp
import Control.Monad.Catch (MonadMask)
import qualified Control.Foldl as Foldl
import qualified Data.Yaml as Y
import Data.Text (pack)
import DA.Sdk.Data.Either.Extra (fmapLeft, swapLefts)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (ExceptT (..), runExceptT, withExceptT)
import Control.Monad.Trans.Reader (ReaderT (..), runReaderT)
import qualified Control.Monad.Logger    as L
import System.Posix.Types (FileMode)
import qualified DA.Sdk.Cli.System as S
import qualified Data.ByteString as BS
import DA.Sdk.Control.Exception.Extra
import DA.Sdk.Cli.Monad.FileSystem.Types
import Data.Text.Encoding

class Monad m => MonadFS m where
    testfile'          :: FilePath -> m Bool
    testdir'           :: FilePath -> m Bool
    mktree'            :: FilePath -> m (Either MkTreeFailed ())
    cptree'            :: FilePath -> FilePath -> m (Either CpTreeFailed ())
    cp'                :: FilePath -> FilePath -> m (Either CpFailed ())
    ls'                :: FilePath -> m (Either LsFailed [T.FilePath])
    datefile'          :: FilePath -> m (Either DateFileFailed T.UTCTime)
    withTempDirectory' :: FilePath -> String -> (FilePath -> m a) -> m (Either WithTempDirectoryFailed a)
    withSystemTempDirectory' :: String -> (FilePath -> m a) -> m (Either WithSysTempDirectoryFailed a)
    decodeYamlFile'    :: Y.FromJSON b => FilePath -> m (Either DecodeYamlFileFailed b)
    encodeYamlFile'    :: Y.ToJSON b => FilePath -> b -> m (Either EncodeYamlFileFailed ())
    unTarGzip'         :: UnTarGzipOptions -> FilePath -> FilePath -> m (Either UnTarGzipFailed ())
    pwd'               :: m (Either PwdFailed FilePath)
    setFileMode'       :: FilePath -> FileMode -> m (Either SetFileModeFailed ())
    writeFile'         :: FilePath -> BS.ByteString -> m (Either WriteFileFailed ())
    touch'             :: FilePath -> m (Either TouchFailed ())
    rm'                :: FilePath -> m (Either RemoveFailed ())
    readFile'          :: FilePath -> m (Either ReadFileFailed BS.ByteString)

    createBinaryStarter' :: FilePath -> FilePath -> m (Either BinaryStarterCreationFailed ())

    default createBinaryStarter' :: MonadIO m => FilePath -> FilePath -> m (Either BinaryStarterCreationFailed ())
    createBinaryStarter' a b = liftIO $ tryWith (BinaryStarterCreationFailed a b) $ S.createBinaryStarter a b

    default touch' :: MonadIO m => FilePath -> m (Either TouchFailed ())
    touch' f = liftIO $ tryWith (TouchFailed f) $ T.touch f

    default rm' :: MonadIO m => FilePath -> m (Either RemoveFailed ())
    rm' f = liftIO $ tryWith (RemoveFailed f) $ T.rm f

    default testfile' :: MonadIO m => FilePath -> m Bool
    testfile' = T.testfile

    default testdir' :: MonadIO m => FilePath -> m Bool
    testdir' = T.testdir

    default mktree' :: MonadIO m => FilePath -> m (Either MkTreeFailed ())
    mktree' path = liftIO $ tryWith (MkTreeFailed path) (T.mktree path)

    default cptree' :: MonadIO m => FilePath -> FilePath -> m (Either CpTreeFailed ())
    cptree' from to = liftIO $ tryWith (CpTreeFailed from to) (T.cptree from to)

    default cp' :: MonadIO m => FilePath -> FilePath -> m (Either CpFailed ())
    cp' from to = liftIO $ tryWith (CpFailed from to) (T.cp from to)

    default ls' :: MonadIO m => FilePath -> m (Either LsFailed [FilePath])
    ls' path = liftIO $ tryWith (LsFailed path) (T.fold (T.ls path) Foldl.list)

    default datefile' :: MonadIO m => FilePath -> m (Either DateFileFailed T.UTCTime)
    datefile' path = liftIO $ tryWith (DateFileFailed path) (T.datefile path)

    default withTempDirectory' :: (MonadMask m, MonadIO m)
                               => FilePath -> String -> (FilePath -> m a)
                               -> m (Either WithTempDirectoryFailed a)
    withTempDirectory' path name m = tryWith (WithTempDirectoryFailed path name)
                                             (Tmp.withTempDirectory (pathToString path) name (m . stringToPath))

    default withSystemTempDirectory' :: (MonadMask m, MonadIO m)
                                     => String -> (FilePath -> m a)
                                     -> m (Either WithSysTempDirectoryFailed a)
    withSystemTempDirectory' name m = tryWith (WithSysTempDirectoryFailed name)
                                              (Tmp.withSystemTempDirectory name (m . stringToPath))

    default decodeYamlFile' :: (MonadIO m, Y.FromJSON b) => FilePath -> m (Either DecodeYamlFileFailed b)
    decodeYamlFile' path = liftIO $ fmapLeft (DecodeYamlFileFailed path)
                                             (Y.decodeFileEither (pathToString path))

    default encodeYamlFile' :: (MonadIO m, Y.ToJSON b) => FilePath -> b -> m (Either EncodeYamlFileFailed ())
    encodeYamlFile' path value = liftIO $ tryWith (EncodeYamlFileFailed path)
                                                  (Y.encodeFile (pathToString path) value)

    default unTarGzip' :: MonadIO m => UnTarGzipOptions -> FilePath -> FilePath -> m (Either UnTarGzipFailed ())
    unTarGzip' opts src dest = liftIO $ fmapLeft (UnTarGzipFailed src dest . pack)
                                                 (G.unTarGzip opts src dest)

    default pwd' :: MonadIO m => m (Either PwdFailed FilePath)
    pwd' = liftIO $ tryWith PwdFailed T.pwd

    default setFileMode' :: MonadIO m => FilePath -> FileMode -> m (Either SetFileModeFailed ())
    setFileMode' file mode = liftIO $ tryWith (SetFileModeFailed file mode)
                                              (S.setFileMode (pathToString file) mode)

    default writeFile' :: MonadIO m => FilePath -> BS.ByteString -> m (Either WriteFileFailed ())
    writeFile' file bs = liftIO $ tryWith (WriteFileFailed file bs)
                                          (BS.writeFile (pathToString file) bs)

    default readFile' :: MonadIO m => FilePath -> m (Either ReadFileFailed BS.ByteString)
    readFile' file = liftIO $ tryWith (ReadFileFailed file)
                                      (BS.readFile (pathToString file))

instance MonadFS IO

instance MonadFS (L.LoggingT IO)

instance MonadFS m => MonadFS (ExceptT e m) where
    touch'            = lift . touch'
    rm'               = lift . rm'
    testfile'         = lift . testfile'
    testdir'          = lift . testdir'
    mktree'           = lift . mktree'
    cptree'         p = lift . cptree' p
    cp'             p = lift . cp' p
    ls'               = lift . ls'
    datefile'         = lift . datefile'
    decodeYamlFile'   = lift . decodeYamlFile'
    encodeYamlFile' p = lift . encodeYamlFile' p
    unTarGzip'    o p = lift . unTarGzip' o p
    pwd'              = lift pwd'
    setFileMode' f    = lift . setFileMode' f
    writeFile' f      = lift . writeFile' f
    readFile' f       = lift $ readFile' f

    withTempDirectory' path name action =
        ExceptT . fmap swapLefts $ withTempDirectory' path name (runExceptT . action)
    withSystemTempDirectory' name action =
        ExceptT . fmap swapLefts $ withSystemTempDirectory' name (runExceptT . action)

    createBinaryStarter' a = lift . createBinaryStarter' a

instance MonadFS m => MonadFS (ReaderT e m) where
    touch'            = lift . touch'
    rm'               = lift . rm'
    testfile'         = lift . testfile'
    testdir'          = lift . testdir'
    mktree'           = lift . mktree'
    cptree'         p = lift . cptree' p
    cp'             p = lift . cp' p
    ls'               = lift . ls'
    datefile'         = lift . datefile'
    decodeYamlFile'   = lift . decodeYamlFile'
    encodeYamlFile' p = lift . encodeYamlFile' p
    unTarGzip'    o p = lift . unTarGzip' o p
    pwd'              = lift pwd'
    setFileMode' f    = lift . setFileMode' f
    writeFile' f      = lift . writeFile' f
    readFile' f       = lift $ readFile' f

    createBinaryStarter' a = lift . createBinaryStarter' a

    withTempDirectory' path name action =
        ReaderT $ \env -> withTempDirectory' path name (flip runReaderT env . action)

    withSystemTempDirectory' name action =
        ReaderT $ \env -> withSystemTempDirectory' name (flip runReaderT env . action)

testfile :: (IsFilePath t, MonadFS m) => FilePathOf t -> m Bool
testfile = testfile' . unwrapFilePathOf

testdir :: (IsDirPath t, MonadFS m) => FilePathOf t -> m Bool
testdir = testdir' . unwrapFilePathOf

mktree :: (IsDirPath t, MonadFS m) => FilePathOf t -> m (Either MkTreeFailed ())
mktree = mktree' . unwrapFilePathOf

cptree :: MonadFS m => T.FilePath -> FilePathOf t -> m (Either CpTreeFailed ())
cptree from to = cptree' from (unwrapFilePathOf to)

cp :: MonadFS m => T.FilePath -> FilePathOf t -> m (Either CpFailed ())
cp from to = cp' from (unwrapFilePathOf to)

ls :: (MonadFS m, IsDirPath a) => FilePathOf a -> m (Either LsFailed [FilePath])
ls = ls' . unwrapFilePathOf

datefile :: MonadFS m => FilePathOf a -> m (Either DateFileFailed T.UTCTime)
datefile = datefile' . unwrapFilePathOf

withTempDirectory :: (IsDirPath p, MonadFS m) => FilePathOf p -> String -> (FilePath -> m a) -> m (Either WithTempDirectoryFailed a)
withTempDirectory = withTempDirectory' . unwrapFilePathOf

decodeYamlFile :: (IsFilePath p, Y.FromJSON b, MonadFS m) => FilePathOf p -> m (Either DecodeYamlFileFailed b)
decodeYamlFile = decodeYamlFile' . unwrapFilePathOf

encodeYamlFile :: (IsFilePath p, Y.ToJSON b, MonadFS m) => FilePathOf p -> b -> m (Either EncodeYamlFileFailed ())
encodeYamlFile = encodeYamlFile' . unwrapFilePathOf

writeFile :: MonadFS m => FilePathOf p -> BS.ByteString -> m (Either WriteFileFailed ())
writeFile file bs = writeFile' (unwrapFilePathOf file) bs

rm :: (MonadFS m, IsFilePath a) => FilePathOf a -> m (Either RemoveFailed ())
rm = rm' . unwrapFilePathOf

createBinaryStarter :: MonadFS m => FilePathOfSomePackageVersionBin -> FilePathOfDaBinaryDirPackageBin -> m (Either BinaryStarterCreationFailed ())
createBinaryStarter a b = createBinaryStarter' (unwrapFilePathOf a) (unwrapFilePathOf b)

readFileUtf8 :: (MonadFS m, IsFilePath a) => FilePathOf a -> m (Either ReadFileUtf8Failed Text)
readFileUtf8 f = runExceptT $ do
    bytes <- withExceptT rErrConvert $ ExceptT $ readFile' f'
    withExceptT uErrConvert $ ExceptT $ return $ decodeUtf8' bytes
  where
    f' = unwrapFilePathOf f
    rErrConvert rE = ReadFileUtf8Failed rE
    uErrConvert e = ReadFileUtf8DecodeFailed f' e
