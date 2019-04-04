-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Functions to deal with the local environement of an installation.
module DA.Sdk.Cli.Env
    ( JdkVersion(..)
    , PythonVersion(..)
    , Binary(..)
    , Error(..)

      -- * Constants
    , requiredMinJdkVersion
    , requiredMinPythonVersion
    , requiredCoreBinaries

      -- * PATH functions
    , warnOnMissingBinaries
    , errorOnMissingBinaries
    , doesBinaryExist
    , findInPath

    , parseJavacVersion
    , parsePythonVersion
    ) where

import           Control.Exception (IOException, SomeException, catch)
import           Control.Exception.Safe (try)
import           DA.Sdk.Prelude
import           DA.Sdk.Cli.Monad
import qualified DA.Sdk.Pretty as P
import           System.Exit    (exitFailure)
import           Turtle
import           Control.Monad.Except
import qualified Control.Monad.Logger as L
import           DA.Sdk.Cli.Monad.MockIO

--------------------------------------------------------------------------------
-- Types
--------------------------------------------------------------------------------

newtype JdkVersion = JdkVersion (Integer, Integer, Integer)
    deriving (Show, Eq)

newtype PythonVersion = PythonVersion (Integer, Integer, Integer)
    deriving (Show, Eq)

-- | Binaries that we require for different commands.
data Binary
    = Javac JdkVersion
    | Python PythonVersion
    | Sbt
    deriving (Show, Eq)

data Error
    = ErrorCouldNotFindBinary FilePath
    | ErrorCouldNotFindPython FilePath PythonVersion
    | ErrorCouldNotParseVersion Text
    | ErrorWrongJdkVersion JdkVersion JdkVersion
    | ErrorWrongPythonVersion PythonVersion PythonVersion
    deriving (Show)

--------------------------------------------------------------------------------
-- Constants
--------------------------------------------------------------------------------

-- | The mimium JDK version we require for all the assistant commands to work.
requiredMinJdkVersion :: JdkVersion
requiredMinJdkVersion = JdkVersion (8, 0, 0)

requiredMinPythonVersion :: PythonVersion
requiredMinPythonVersion = PythonVersion (3, 6, 2)

-- | The binaries we need for the core functionality of the assistant to work.
requiredCoreBinaries :: [Binary]
requiredCoreBinaries =
    [ Javac requiredMinJdkVersion
    , Python requiredMinPythonVersion
    ]

--------------------------------------------------------------------------------
-- PATH functions
--------------------------------------------------------------------------------

-- | Check if a list of required binaries can be found, warn if not.
warnOnMissingBinaries :: (MockIO m, L.MonadLogger m) => [Binary] -> m ()
warnOnMissingBinaries binaries = checkForMissingBinaries binaries >>=
    \errors -> forM_ errors (logWarn . P.renderPlain . P.pretty)

-- | Require a binary, terminate the program with an error if the binary cannot
-- be found.
errorOnMissingBinaries :: [Binary] -> CliM ()
errorOnMissingBinaries binaries = checkForMissingBinaries binaries >>= \errors -> do
    forM_ errors (logError . P.renderPlain . P.pretty)
    unless (null errors) $ liftIO exitFailure

checkForMissingBinaries :: MockIO m => [Binary] -> m [Error]
checkForMissingBinaries binaries =
    catMaybes <$> forM binaries (fmap fun . doesBinaryExist)
  where
    fun = \case
        Left er -> Just er
        Right () -> Nothing

-- | Check if a binary exists on the path, returns an error message if not.
-- Note(ng): It probably makes sense to have an error type that explains what
-- went wrong so that we can later print it as we need it.
doesBinaryExist :: MockIO m => Binary -> m (Either Error ())
doesBinaryExist = \case
    Javac rV@(JdkVersion requiredVersion) -> javac >>= \case
        Left er -> pure $ Left er
        Right fV@(JdkVersion foundVersion) ->
            if foundVersion >= requiredVersion then
                pure $ Right ()
            else
                pure $ Left $ ErrorWrongJdkVersion rV fV

    Python rV@(PythonVersion requiredVersion) -> python >>= \case
        Left er -> pure $ Left er
        Right fV@(PythonVersion foundVersion) ->
            if foundVersion >= requiredVersion then
                pure $ Right ()
            else
                pure $ Left $ ErrorWrongPythonVersion rV fV

    Sbt -> void <$> findInPath "sbt"

-- | Searches for a binary in the 'PATH'.
findInPath ::
       MockIO m
    => FilePath
    -- ^ Name of the binary to search for
    -> m (Either Error FilePath)
findInPath path = mbToEither <$> (mockWhich $ which path)
  where
    mbToEither = maybe (Left $ ErrorCouldNotFindBinary path) Right

-- | Run a command with parameters and returns stdout and stderr concatenated
stderrOrOut :: MockIO m => Text -> [Text] -> m (Either Error Text)
stderrOrOut cmd params = mockIO (ConstMock $ Right "") $ do
    res :: Either SomeException (ExitCode, Text, Text)
        <- liftIO $ try $ procStrictWithErr cmd params empty
    case res of
      Right (_status, sout, serr) ->
        return $ Right (sout <> serr)
      Left _err ->
        return $ Left $ ErrorCouldNotFindBinary $ textToPath cmd

-- Javac
--------------------------------------------------------------------------------

-- | Javac versions differ from SemVersions in that they use an @_@ for
-- prerelease information.
parseJavacVersion :: Text -> Either Error JdkVersion
parseJavacVersion versionText =
    case match pattern versionText of
        [] ->
            Left $ ErrorCouldNotParseVersion versionText
        versionMatch : _xs ->
            Right versionMatch
  where
    pattern = do
        void chars
        void $ text "javac"
        void space
        major <- decimal
        minor <- optional $ char '.' *> decimal
        patch <- optional $ char '.' *> decimal
        void chars
        if major == 1 then
            pure
                $ JdkVersion
                    ( fromMaybe 0 minor
                    , fromMaybe 0 patch
                    , 0
                    )
        else
            pure
                $ JdkVersion
                    ( major
                    , fromMaybe 0 minor
                    , fromMaybe 0 patch
                    )

-- | Format a javac version.
formatJavacVersion :: JdkVersion -> Text
formatJavacVersion (JdkVersion (major, _minor, _patch)) =
    format ("JDK "%d) major

parsePythonVersion :: Text -> Either Error PythonVersion
parsePythonVersion versionText =
    case match pattern versionText of
        [] ->
            Left $ ErrorCouldNotParseVersion versionText
        versionMatch : _xs ->
            Right versionMatch
  where
    pattern = do
        void chars
        void $ asciiCI "python"
        void space
        major <- decimal
        minor <- optional $ char '.' *> decimal
        patch <- optional $ char '.' *> decimal
        void chars
        pure $ PythonVersion (major, fromMaybe 0 minor, fromMaybe 0 patch)

formatPythonVersion :: PythonVersion -> Text
formatPythonVersion (PythonVersion (major, minor, patch)) =
    format ("Python "%d%"."%d%"."%d) major minor patch

-- | Check if a @javac@ can be found on the path and return it's version.
-- Note: @javac -version@ outputs version information on stderr
-- (at least the openjdk version).
javac :: MockIO m => m (Either Error JdkVersion)
javac = runExceptT $ do
    t <- ExceptT $ stderrOrOut "javac" ["-version"]
    ExceptT $ return $ parseJavacVersion t

-- | Check if @python3@ can be found on the path and return it's version.
python :: MockIO m => m (Either Error PythonVersion)
python = runExceptT $ do
    vTxt <- ExceptT $ mockPythonVersion $ catch
                (Right . snd <$> procStrict "python3" ["--version"] empty)
                (\(_er :: IOException) -> pure $ Left $
                    ErrorCouldNotFindPython "python3" requiredMinPythonVersion)
    ExceptT $ return $ parsePythonVersion vTxt

--------------------------------------------------------------------------------
-- Pretty Printing
--------------------------------------------------------------------------------

instance P.Pretty Error where
    pretty = \case
        ErrorCouldNotFindBinary filePath -> P.fillSep
            [ P.reflow "Couldn't find binary"
            , "\"" <> P.p filePath <> "\""
            , P.reflow "in path."
            , P.reflow "Please make sure that it is installed, and that "
            , "\"" <> P.p filePath <> "\" is on your path."
            ]
        ErrorCouldNotFindPython filePath pythonVersion -> P.fillSep
            [ P.reflow "Couldn't find binary"
            , "\"" <> P.p filePath <> "\""
            , P.reflow "in path."
            , P.reflow "Please make sure that Python is installed, and that "
            , "\"" <> P.p filePath <> "\" is on your path."
            , P.reflow "At least "
            , P.t (formatPythonVersion pythonVersion)
            , P.reflow "is required."
            ]
        ErrorCouldNotParseVersion versionText -> P.fillSep
            [ P.reflow "Couldn't parse version"
            , P.pretty versionText
            ]
        ErrorWrongJdkVersion requiredVersion foundVersion ->
            P.vsep
                [ P.reflow "The installed JDK version is not supported"
                , "Installed version: " <> P.t (formatJavacVersion foundVersion)
                , "Min required version: " <> P.t (formatJavacVersion requiredVersion)
                ]
        ErrorWrongPythonVersion requiredVersion foundVersion ->
            P.vsep $
                [ P.reflow "The installed Python version is not supported."
                , "Installed version: " <> P.t (formatPythonVersion foundVersion)
                , "Minimal required version: " <> P.t (formatPythonVersion requiredVersion)
                ]
