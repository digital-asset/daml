-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.Validate (validateDar) where

import Control.Exception.Extra (errorIO)
import Control.Lens
import Control.Monad (forM_)
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Optics as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as L
import qualified Data.Text as T
import Language.LSP.Types
import Language.LSP.Types.Lens

import DA.Daml.Compiler.ExtractDar (extractDar,ExtractedDar(..))
import DA.Daml.LF.Ast.World (initWorldSelf)
import qualified DA.Daml.LF.TypeChecker as TC (checkPackage)

data ValidationError
  = VeArchiveError FilePath Archive.ArchiveError
  | VeTypeError [Diagnostic]

instance Show ValidationError where
  show = \case
    VeArchiveError fp err -> unlines
      [ "Invalid DAR."
      , "DALF entry cannot be decoded: " <> fp
      , show err ]
    VeTypeError diags -> unlines $
      [ "Invalid DAR."
      , "The DAR is not well typed."
      ] <> map (T.unpack . view message) diags
      -- Right now we treat all diagnostics the same
      -- for the purposes of validation and do not
      -- differentiate between warnings and errors.


validationError :: ValidationError -> IO a
validationError = errorIO . show

-- | Validate a loaded DAR: that all DALFs are well-typed and consequently that the DAR is closed
validateDar :: FilePath -> IO Int
validateDar inFile = do
  ExtractedDar{edDalfs} <- extractDar inFile
  extPackages <- mapM (decodeDalfEntry Archive.DecodeAsDependency) edDalfs
  validateWellTyped extPackages
  return $ L.length extPackages

validateWellTyped :: [LF.ExternalPackage] -> IO ()
validateWellTyped extPackages = do
  forM_ extPackages $ \extPkg -> do
    let rewriteToSelf (LF.PRImport pkgId)
          | pkgId == LF.extPackageId extPkg = LF.PRSelf
        rewriteToSelf ref = ref
    let self = over LF.packageRefs rewriteToSelf (LF.extPackagePkg extPkg)
    let world = initWorldSelf extPackages self
    let version = LF.packageLfVersion self
    case TC.checkPackage world version of
        [] -> pure ()
        diags -> validationError (VeTypeError diags)

decodeDalfEntry :: Archive.DecodingMode -> ZipArchive.Entry -> IO LF.ExternalPackage
decodeDalfEntry decodeAs entry = do
  let bs = BSL.toStrict $ ZipArchive.fromEntry entry
  case Archive.decodeArchive decodeAs bs of
    Left err -> validationError $ VeArchiveError (ZipArchive.eRelativePath entry) err
    Right (pid,package) -> return $ LF.ExternalPackage pid package
