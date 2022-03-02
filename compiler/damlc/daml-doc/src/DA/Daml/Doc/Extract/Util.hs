-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.Util
    ( module DA.Daml.Doc.Extract.Util
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Anchor
import DA.Daml.Doc.Extract.Types

import Control.Monad (guard)
import Data.Char (isSpace)
import qualified Data.Text as T

import "ghc-lib" GHC
import "ghc-lib-parser" Module
import "ghc-lib-parser" OccName
import "ghc-lib-parser" Id
import "ghc-lib-parser" Name
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" TyCon
import "ghc-lib-parser" TyCoRep

-- render type variables as text (ignore kind information)
tyVarText :: HsTyVarBndr GhcPs -> T.Text
tyVarText arg = case arg of
                  UserTyVar _ (L _ idp)         -> packRdrName idp
                  KindedTyVar _ (L _ idp) _kind -> packRdrName idp
                  XTyVarBndr _
                    -> error "unexpected X thing"

-- | Same as 'docToText' but for module headers (deals with HAS_DAML_VERSION_HEADER).
moduleDocToText :: HsDocString -> DocText
moduleDocToText = docToText' . removeVersionHeaderAnnotation . T.pack . unpackHDS

-- | Converts and trims the bytestring of a doc. decl to Text.
docToText :: HsDocString -> DocText
docToText = docToText' . T.pack . unpackHDS

-- | Remove the HAS_DAML_VERSION_HEADER annotation inserted by our patched ghc parser.
removeVersionHeaderAnnotation :: T.Text -> T.Text
removeVersionHeaderAnnotation doc
    = maybe doc T.stripStart (T.stripPrefix "HAS_DAML_VERSION_HEADER" doc)

docToText' :: T.Text -> DocText
docToText' = DocText . T.strip . T.unlines . go . T.lines
  where
    -- For a haddock comment of the form
    --
    -- -- | First line
    -- --   second line
    -- --   third line
    --
    -- we strip all whitespace from the first line and then on the following
    -- lines we strip at most as much whitespace as we find on the next
    -- non-whitespace line. In the example above, this would result
    -- in the string "First line\nsecond line\n third line".
    -- Trailing whitespace is always stripped.
    go :: [T.Text] -> [T.Text]
    go [] = []
    go (x:xs) = case span isWhitespace xs of
      (_, []) -> [T.strip x]
      (allWhitespace, ls@(firstNonWhitespace : _)) ->
        let limit = T.length (T.takeWhile isSpace firstNonWhitespace)
        in T.strip x : map (const "") allWhitespace ++ map (stripLine limit ) ls
    isWhitespace = T.all isSpace
    stripLine limit = T.stripEnd . stripLeading limit
    stripLeading limit = T.pack . map snd . dropWhile (\(i, c) -> i < limit && isSpace c) . zip [0..] . T.unpack

-- | Turn an Id into Text by taking the unqualified name it represents.
packId :: Id -> T.Text
packId = packName . idName

-- | Turn a Name into Text by taking the unqualified name it represents.
packName :: Name -> T.Text
packName = packOccName . nameOccName

-- | Turn an OccName into Text by taking the unqualified name it represents.
packOccName :: OccName -> T.Text
packOccName = T.pack . occNameString

-- | Turn a RdrName into Text by taking the unqualified name it represents.
packRdrName :: RdrName -> T.Text
packRdrName = packOccName . rdrNameOcc

-- | Turn a FieldOcc into Text by taking the unqualified name it represents.
packFieldOcc :: FieldOcc p -> T.Text
packFieldOcc = packRdrName . unLoc . rdrNameFieldOcc

-- | Turn a TyLit into a text.
packTyLit :: TyLit -> T.Text
packTyLit (NumTyLit x) = T.pack (show x)
packTyLit (StrTyLit x) = T.pack (show x)

-- | Turn a GHC Module into a Modulename. (Unlike the above functions,
-- we only ever want this to be a Modulename, so no reason to return
-- Text.)
getModulename :: Module -> Modulename
getModulename = Modulename . T.pack . moduleNameString . moduleName


-- | Get package name from unit id.
modulePackage :: Module -> Maybe Packagename
modulePackage mod =
    case moduleUnitId mod of
        unitId@(DefiniteUnitId _) ->
            Just . Packagename . T.pack . unitIdString $ unitId
        _ -> Nothing

-- | Create an anchor from a TyCon.
tyConAnchor :: DocCtx -> TyCon -> Maybe Anchor
tyConAnchor DocCtx{..} tycon = do
    let ghcName = tyConName tycon
        name = Typename . packName $ ghcName
        mod = maybe dc_modname getModulename (nameModule_maybe ghcName)
        anchorFn
            | isClassTyCon tycon = classAnchor
            | otherwise = typeAnchor
    Just (anchorFn mod name)

-- | Create a (possibly external) reference from a TyCon.
tyConReference :: DocCtx -> TyCon -> Maybe Reference
tyConReference ctx@DocCtx{..} tycon = do
    referenceAnchor <- tyConAnchor ctx tycon
    let ghcName = tyConName tycon
        referencePackage = do
            guard (not (nameIsHomePackage dc_ghcMod ghcName))
            mod <- nameModule_maybe ghcName
            modulePackage mod
    Just Reference {..}

-- | Extract a potentially qualified typename from a TyCon.
tyConTypename :: DocCtx -> TyCon -> Typename
tyConTypename DocCtx{..} tycon =
    let ExtractOptions{..} = dc_extractOptions
        ghcName = tyConName tycon
        qualify =
            case eo_qualifyTypes of
                QualifyTypesAlways -> True
                QualifyTypesInPackage -> nameIsHomePackageImport dc_ghcMod ghcName
                QualifyTypesNever -> False

        moduleM = guard qualify >> nameModule_maybe ghcName
        modNameM = getModulename <$> moduleM
        simplifyModName
            | eo_simplifyQualifiedTypes = dropCommonModulePrefix dc_modname
            | otherwise = id
        prefix = maybe "" ((<> ".") . unModulename . simplifyModName) modNameM
    in Typename (prefix <> packName ghcName)

-- | Drop common module name prefix, returning the second module name
-- sans the module prefix it has in common with the first module name.
-- This will not return an empty module name however (unless given an
-- empty module name to start).
--
-- This function respects the atomicity of the module names between
-- periods. For instance @dropCommonModulePrefix "Foo.BarBaz" "Foo.BarSpam"@
-- will evaluate to @"BarSpam"@, not @"Spam"@.
dropCommonModulePrefix :: Modulename -> Modulename -> Modulename
dropCommonModulePrefix (Modulename baseMod) (Modulename targetMod) =
    Modulename . T.intercalate "." $
        aux (T.splitOn "." baseMod) (T.splitOn "." targetMod)
  where
    aux :: [T.Text] -> [T.Text] -> [T.Text]
    aux _ [x] = [x]
    aux (x:xs) (y:ys) | x == y = aux xs ys
    aux _ p = p
