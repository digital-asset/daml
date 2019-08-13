-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module processes export lists and then answers the question
-- "is this *thing* exported?"
module DA.Daml.Doc.Extract.Exports
    ( ExportSet
    , extractExports
    , exportsType
    , exportsConstr
    , exportsFunction
    , exportsField
    ) where

import DA.Daml.Doc.Types as DD

import "ghc-lib" GHC
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" OccName
import "ghc-lib-parser" FieldLabel
import "ghc-lib-parser" FastString

import qualified Data.Set as Set
import qualified Data.Text as T

-- | Set of module exports.
--
-- Unlike Haddock, we don't ask the export list to dictate the order
-- of docs; damldocs imposes its own order. So we can treat the export
-- list as a set instead.
data ExportSet
    = ExportEverything
    | ExportOnly !(Set.Set ExportedItem)

-- | Particular exported item. We don't particularly care
-- about re-exported modules for now, but we want to know
-- if a module re-exports itself so we have a way to track
-- it here.
data ExportedItem
    = ExportedType !Typename
        -- ^ type is exported
    | ExportedTypeAll !Typename
        -- ^ all constructors and fields for a type are exported
    | ExportedConstr !Typename
        -- ^ constructor is exported
    | ExportedFunction !Fieldname
        -- ^ function or field is exported
    | ExportedModule !GHC.ModuleName
        -- ^ module is reexported
    deriving (Eq, Ord)

-- | Get set of exports from parsed module.
--
-- We work with the parsed module here rather than the typechecked
-- module because damldocs generally works with parsed AST names.
extractExports :: ParsedModule -> ExportSet
extractExports pm
    | (L _ ps) <- pm_parsed_source pm
    , Just (L _ modName) <- hsmodName ps
    , Just (L _ lies) <- hsmodExports ps
    , exportedItems <- Set.fromList (concatMap (extractExportedItem modName) lies)
    = if ExportedModule modName `Set.member` exportedItems
        then ExportEverything
        else ExportOnly exportedItems

    | otherwise
    = ExportEverything

extractExportedItem :: GHC.ModuleName -> LIE GhcPs -> [ExportedItem]
extractExportedItem modName (L _ ie) = exportIE ie
  where
    exportIE :: IE GhcPs -> [ExportedItem]
    exportIE = \case
        IEVar _ (L _ n) -> exportIEWrappedWith exportOccName n
        IEThingAbs _ (L _ n) -> exportIEWrappedWith exportOccName n
        IEThingAll _ (L _ n) -> addTypeAll $ exportIEWrappedWith exportOccName n
        IEThingWith _ (L _ n) (IEWildcard _) _ _ ->
            addTypeAll $ exportIEWrappedWith exportOccName n
        IEThingWith _ (L _ n) NoIEWildcard things fields -> concat
            [ exportIEWrappedWith exportOccName n
            , concatMap (exportIEWrappedWith exportConstr . unLoc) things
            , [ ExportedFunction . Fieldname . T.pack . unpackFS $ x
              | L _ (FieldLabel x _ _) <- fields
              ]
            ]
        IEModuleContents _ (L _ n) -> [ExportedModule n]
        IEGroup _ _ _ -> []
        IEDoc _ _ -> []
        IEDocNamed _ _ -> []
        XIE _ -> []

    exportIEWrappedWith :: (OccName -> [ExportedItem]) -> IEWrappedName RdrName -> [ExportedItem]
    exportIEWrappedWith f = \case
        IEName (L _ rdrName) -> exportRdrNameWith f rdrName
        IEType (L _ rdrName) -> exportRdrNameWith f rdrName
        IEPattern _ -> []

    exportRdrNameWith :: (OccName -> [ExportedItem]) -> RdrName -> [ExportedItem]
    exportRdrNameWith f = \case
        Unqual n -> f n
        Qual m n ->
            if m == modName
                then f n
                else []
        Orig _ _ -> []
        Exact _ -> []

    exportOccName :: OccName -> [ExportedItem]
    exportOccName n
        | isVarOcc n = [ExportedFunction . Fieldname . T.pack . occNameString $ n]
        | otherwise = [ExportedType . Typename . T.pack . occNameString $ n]

    exportConstr :: OccName -> [ExportedItem]
    exportConstr n
        | isVarOcc n = [ExportedFunction . Fieldname . T.pack . occNameString $ n]
        | otherwise = [ExportedConstr . Typename . T.pack . occNameString $ n]

    addTypeAll :: [ExportedItem] -> [ExportedItem]
    addTypeAll = concatMap $ \case
        ExportedType ty -> [ExportedType ty, ExportedTypeAll ty]
        item -> [item]

exportsType :: ExportSet -> Typename -> Bool
exportsType ExportEverything _ = True
exportsType (ExportOnly xs) n = Set.member (ExportedType n) xs

exportsConstr :: ExportSet -> Typename -> Typename -> Bool
exportsConstr ExportEverything _ _ = True
exportsConstr (ExportOnly xs) ty constr =
    Set.member (ExportedTypeAll ty) xs
    || Set.member (ExportedConstr constr) xs

exportsFunction :: ExportSet -> Fieldname -> Bool
exportsFunction ExportEverything _ = True
exportsFunction (ExportOnly xs) n = Set.member (ExportedFunction n) xs

exportsField :: ExportSet -> Typename -> Fieldname -> Bool
exportsField ExportEverything _ _ = True
exportsField (ExportOnly xs) ty field =
    Set.member (ExportedTypeAll ty) xs
    || Set.member (ExportedFunction field) xs
