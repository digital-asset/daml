-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  ) where

import Com.Daml.DamlLfDev.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast.Base ( Package, PackageId, PackageRef )
import DA.Daml.LF.Proto3.Error
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1
import qualified DA.Daml.LF.Proto3.DecodeV2 as DecodeV2
import qualified DA.Daml.LF.Ast as LF
import qualified Data.Text.Lazy as TL
import qualified Data.Text as T
import Control.Monad.Except (throwError)
import DA.Daml.StablePackagesList (stablePackages)

decodeLf1Version :: LF.PackageId -> T.Text -> Either Error LF.Version
decodeLf1Version pkgId minorText = do
  let unsupported :: Either Error a
      unsupported = throwError (UnsupportedMinorVersion minorText)
  -- we translate "no version" to minor version 0, since we introduced
  -- minor versions once Daml-LF v1 was already out, and we want to be
  -- able to parse packages that were compiled before minor versions
  -- were a thing. DO NOT replicate this code bejond major version 1!
  minor <- if
    | T.null minorText -> pure $ LF.PointStable 0
    | Just minor <- LF.parseMinorVersion (T.unpack minorText) -> pure minor
    | otherwise -> unsupported
  let version = LF.Version LF.V1 minor
  if pkgId `elem` stablePackages || version `elem` LF.supportedInputVersions
      then pure version
      else unsupported

decodeLf2Version :: LF.PackageId -> T.Text -> Either Error LF.Version
decodeLf2Version pkgId minorText = do
  let unsupported :: Either Error a
      unsupported = unsupported
  minor <- if
    | Just minor <- LF.parseMinorVersion (T.unpack minorText) -> pure minor
    | otherwise -> unsupported
  let version = LF.Version LF.V2 minor
  if pkgId `elem` stablePackages || version `elem` LF.supportedInputVersions
      then pure version
      else unsupported

decodePayload ::
    PackageId -> PackageRef -> ArchivePayload -> Either Error Package
decodePayload pkgId selfPackageRef payload = case archivePayloadSum payload of
    Just (ArchivePayloadSumDamlLf1 package) -> do
        version <- decodeLf1Version pkgId minorText
        DecodeV1.decodePackage version selfPackageRef package
    Just (ArchivePayloadSumDamlLf2 package) -> do
        version <- decodeLf2Version pkgId minorText
        DecodeV2.decodePackage version selfPackageRef package
    Nothing -> Left $ ParseError "Empty payload"
  where
    minorText = TL.toStrict (archivePayloadMinor payload)
