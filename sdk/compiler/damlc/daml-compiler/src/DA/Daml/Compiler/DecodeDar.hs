-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DecodeDar
  ( DecodedDar(..)
  , DecodedDalf(..)
  , decodeDar
  , decodeDalf
  ) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Either.Combinators
import Data.Set (Set)
import qualified Data.Set as Set
import "ghc-lib-parser" Module (UnitId)

import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.ExtractDar (ExtractedDar(..))
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Pretty

data DecodedDar = DecodedDar
    { mainDalf :: DecodedDalf
    , dalfs :: [DecodedDalf]
    -- ^ Like in the MANIFEST.MF definition, this includes
    -- the main dalf.
    }

decodeDar :: Set LF.PackageId -> ExtractedDar -> Either String DecodedDar
decodeDar dependenciesInPkgDb ExtractedDar{..} = do
    mainDalf <-
        decodeDalf
            dependenciesInPkgDb
            (ZipArchive.eRelativePath edMain)
            (BSL.toStrict $ ZipArchive.fromEntry edMain)
    otherDalfs <-
        mapM decodeEntry $
            filter
                (\e -> ZipArchive.eRelativePath e /= ZipArchive.eRelativePath edMain)
                edDalfs
    let dalfs = mainDalf : otherDalfs
    pure DecodedDar{..}
  where
    decodeEntry entry =         decodeDalf
            dependenciesInPkgDb
            (ZipArchive.eRelativePath entry)
            (BSL.toStrict $ ZipArchive.fromEntry entry)

data DecodedDalf = DecodedDalf
    { decodedDalfPkg :: LF.DalfPackage
    , decodedUnitId :: UnitId
    }

decodeDalf :: Set LF.PackageId -> FilePath -> BS.ByteString -> Either String DecodedDalf
decodeDalf dependenciesInPkgDb path bytes = do
    (pkgId, package) <-
        mapLeft DA.Pretty.renderPretty $
        Archive.decodeArchive Archive.DecodeAsDependency bytes
    -- daml-prim and daml-stdlib are somewhat special:
    --
    -- We always have daml-prim and daml-stdlib from the current SDK and we
    -- cannot control their unit id since that would require recompiling them.
    -- However, we might also have daml-prim and daml-stdlib in a different version
    -- in a DAR we are depending on. Luckily, we can control the unit id there.
    -- To avoid colliding unit ids which will confuse GHC (or rather hide
    -- one of them), we instead include the package hash in the unit id.
    --
    -- In principle, we can run into the same issue if you combine "dependencies"
    -- (which have precompiled interface files) and
    -- "data-dependencies". However, there you can get away with changing the
    -- package name and version to change the unit id which is not possible for
    -- daml-prim.
    --
    -- If the version of daml-prim/daml-stdlib in a data-dependency is the same
    -- as the one we are currently compiling against, we don’t need to apply this
    -- hack.
    let (name, mbVersion) = case LF.packageMetadataFromFile path package pkgId of
            (LF.PackageName "daml-prim", Nothing)
                | pkgId `Set.notMember` dependenciesInPkgDb ->
                  (LF.PackageName ("daml-prim-" <> LF.unPackageId pkgId), Nothing)
            (LF.PackageName "daml-stdlib", _)
                | pkgId `Set.notMember` dependenciesInPkgDb ->
                  (LF.PackageName ("daml-stdlib-" <> LF.unPackageId pkgId), Nothing)
            (name, mbVersion) -> (name, mbVersion)
    pure DecodedDalf
        { decodedDalfPkg = LF.DalfPackage pkgId (LF.ExternalPackage pkgId package) bytes
        , decodedUnitId = pkgNameVersion name mbVersion
        }
