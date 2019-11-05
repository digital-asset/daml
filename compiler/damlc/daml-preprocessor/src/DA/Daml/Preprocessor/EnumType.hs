-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--
-- This module is dedicated to renaming single constructor enum types
-- by adding the prefix "DamlEnum$", and creating a type alias. The
-- point of this is so we can detect single constructor enum types
-- during LFConversion step. When we rename the type, we also need
-- to fix this module's export lists.

module DA.Daml.Preprocessor.EnumType
    ( enumTypePreprocessor
    , enumPrefix
    ) where

import Data.List.Extra (mconcatMap)
import qualified Data.Map.Strict as M

import "ghc-lib" GHC
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" OccName

enumPrefix :: String
enumPrefix = "DamlEnum$"

enumTypePreprocessor :: ParsedSource -> ParsedSource
enumTypePreprocessor (L l src) =
    let (decls, renamedTypes) = mconcatMap fixEnumTypeDecl (hsmodDecls src)
        exports = fixExports (M.fromList renamedTypes) (hsmodExports src)
    in L l src { hsmodDecls = decls, hsmodExports = exports }

fixExports :: M.Map RdrName RdrName -> Maybe (Located [LIE GhcPs]) -> Maybe (Located [LIE GhcPs])
fixExports typeMap = \case
    Nothing -> Nothing
    Just (L loc exports) -> Just (L loc (concatMap (fixExport typeMap) exports))

fixExport :: M.Map RdrName RdrName -> LIE GhcPs -> [LIE GhcPs]
fixExport typeMap (L l ie) = map (L l) $ case ie of

    IEThingAbs x n | Just newName <- M.lookup (lieWrappedName n) typeMap ->
        [IEThingAbs noExt n, IEThingAbs x (replaceLWrappedName n newName)]

    IEThingAll x n | Just newName <- M.lookup (lieWrappedName n) typeMap ->
        [IEThingAbs noExt n, IEThingAll x (replaceLWrappedName n newName)]

    IEThingWith x n w ns fs | Just newName <- M.lookup (lieWrappedName n) typeMap ->
        [IEThingAbs noExt n, IEThingWith x (replaceLWrappedName n newName) w ns fs]

    ie -> [ie]


fixEnumTypeDecl :: LHsDecl GhcPs -> ([LHsDecl GhcPs], [(RdrName, RdrName)])
fixEnumTypeDecl (L l decl)
    | TyClD xtc (DataDecl xdd (L nameLoc name) dtyvars dfixity ddefn) <- decl
    , HsQTvs _ [] <- dtyvars
    , HsDataDefn {dd_cons = [con]} <- ddefn
    , PrefixCon [] <- con_args (unLoc con)
    , Unqual oname <- name
    =   let newName = Unqual (mkOccName (occNameSpace oname) (enumPrefix <> occNameString oname))
            newDecls =
                [ L l (TyClD xtc (DataDecl xdd (L nameLoc newName) dtyvars dfixity ddefn))
                , L l (TyClD xtc (SynDecl noExt (L nameLoc name) dtyvars dfixity
                    (L nameLoc (HsTyVar noExt NotPromoted (L nameLoc newName)))))
                ]
            newPairs = [(name, newName)]
        in (newDecls, newPairs)

fixEnumTypeDecl ldecl = ([ldecl], [])
