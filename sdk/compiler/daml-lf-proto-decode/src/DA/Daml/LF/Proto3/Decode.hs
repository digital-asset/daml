-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  , DecodeV2.decodePackage
  , DecodeV2.decodeSinglePackageModule
  ) where

import Com.Digitalasset.Daml.Lf.Archive.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))

import qualified Proto3.Suite as Proto

import           DA.Daml.LF.Ast.Base     ( Package, PackageId, SelfOrImportedPackageId )
import           DA.Daml.LF.Proto3.Error
import qualified DA.Daml.LF.Proto3.DecodeV2 as DecodeV2
import qualified DA.Daml.LF.Ast             as LF
import           DA.Daml.StablePackagesList (stablePackages)

import           Control.Monad.Except (throwError)
import           Control.Lens         (over, _Left)
import Data.Int (Int32)
import qualified Data.Text.Lazy       as TL
import qualified Data.Text            as T

decodeLfVersion :: LF.MajorVersion -> LF.PackageId -> T.Text -> Int32 -> Either Error LF.Version
decodeLfVersion major pkgId minorText patchInt = do
  let unsupportedMinor :: Either Error a
      unsupportedMinor = throwError (UnsupportedMinorVersion minorText)
      unsupportedPatch :: LF.MinorVersion -> Int32 -> Either Error a
      unsupportedPatch minor patch = throwError (UnsupportedPatchVersion minor patch)
  minor <- if
    | Just minor <- LF.parseMinorVersion (T.unpack minorText) -> pure minor
    | otherwise -> unsupportedMinor
  _ <- if patchInt == 0 then pure () else unsupportedPatch minor patchInt
  let version = LF.Version major minor
  if pkgId `elem` stablePackages || version `elem` LF.supportedInputVersions
      then pure version
      else unsupportedMinor

decodePayload ::
    PackageId -> SelfOrImportedPackageId -> ArchivePayload -> Either Error Package
decodePayload pkgId selfPackageRef payload = case archivePayloadSum payload of
    Just (ArchivePayloadSumDamlLf1 _) -> do
         Left $  ParseError "Lf1 is not supported"
    Just (ArchivePayloadSumDamlLf2 packageBytes) -> do
        package <- over _Left (ParseError . show) $ Proto.fromByteString packageBytes
        version <- decodeLfVersion LF.V2 pkgId minorText patchInt
        DecodeV2.decodePackage version selfPackageRef package
    Nothing -> Left $ ParseError "Empty payload"
  where
    minorText = TL.toStrict (archivePayloadMinor payload)
    patchInt = archivePayloadPatch payload
