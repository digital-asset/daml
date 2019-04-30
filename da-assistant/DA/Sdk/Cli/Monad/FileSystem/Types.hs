-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}

module DA.Sdk.Cli.Monad.FileSystem.Types(module DA.Sdk.Cli.Monad.FileSystem.Types) where

import DA.Sdk.Prelude
import qualified Data.Yaml as Y
import qualified Control.Exception.Safe as E
import qualified Data.ByteString as BS
import System.Posix.Types (FileMode)
import Data.Text.Encoding.Error

data MkTreeFailed            = MkTreeFailed            FilePath          IOError          deriving (Show)
data CpTreeFailed            = CpTreeFailed            FilePath FilePath IOError          deriving (Show)
data CpFailed                = CpFailed                FilePath FilePath IOError          deriving (Show)
data LsFailed                = LsFailed                FilePath          IOError          deriving (Show)
data DateFileFailed          = DateFileFailed          FilePath          IOError          deriving (Show)
data WithTempDirectoryFailed = WithTempDirectoryFailed FilePath String   IOError          deriving (Show)
data WithSysTempDirectoryFailed = WithSysTempDirectoryFailed String      IOError          deriving (Show)
data DecodeYamlFileFailed    = DecodeYamlFileFailed    FilePath          Y.ParseException deriving (Show)
data EncodeYamlFileFailed    = EncodeYamlFileFailed    FilePath          E.SomeException  deriving (Show)
data UnTarGzipFailed         = UnTarGzipFailed         FilePath FilePath Text             deriving (Show)
data PwdFailed               = PwdFailed                                 IOError          deriving (Show)
data SetFileModeFailed       = SetFileModeFailed       FilePath FileMode E.SomeException  deriving (Show)
data WriteFileFailed         = WriteFileFailed FilePath BS.ByteString E.SomeException     deriving (Show)
data TouchFailed             = TouchFailed FilePath E.SomeException deriving (Show)
data RemoveFailed            = RemoveFailed FilePath E.SomeException deriving (Show)
data BinaryStarterCreationFailed = BinaryStarterCreationFailed FilePath FilePath E.SomeException deriving (Show)
data ReadFileFailed          = ReadFileFailed FilePath IOError deriving (Show)
data ReadFileUtf8Failed      = ReadFileUtf8DecodeFailed FilePath UnicodeException
                             | ReadFileUtf8Failed ReadFileFailed
                             deriving (Show)
