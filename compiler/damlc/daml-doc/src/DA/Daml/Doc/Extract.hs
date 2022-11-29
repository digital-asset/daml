-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module extracts docs from Daml modules. It does so by reading
-- haddock-style comments from the parsed syntax tree and correlating them
-- with definitions in the typechecked module in order to obtain accurate
-- type information.
module DA.Daml.Doc.Extract
    ( ExtractOptions (..)
    , QualifyTypes (..)
    , defaultExtractOptions
    , extractDocs
    ) where

import DA.Daml.Doc.Types as DDoc
import DA.Daml.Doc.Anchor as DDoc

import DA.Daml.Doc.Extract.Types
import DA.Daml.Doc.Extract.Util
import DA.Daml.Doc.Extract.Exports
import DA.Daml.Doc.Extract.Templates
import DA.Daml.Doc.Extract.TypeExpr

import DA.Daml.Options.Types

import qualified DA.Service.Logger.Impl.Pure as Logger

import Development.IDE.Core.IdeState.Daml
import qualified Development.IDE.Core.Service as Service
import qualified Development.IDE.Core.Shake as Service
import qualified Development.IDE.Core.Rules     as Service
import qualified Development.IDE.Core.Rules.Daml as Service
import qualified Development.IDE.Core.RuleTypes.Daml as Service
import qualified Development.IDE.Core.OfInterest as Service
import Development.IDE.Types.Location

import "ghc-lib" GHC
import "ghc-lib-parser" TyCon
import "ghc-lib-parser" ConLike
import "ghc-lib-parser" DataCon
import "ghc-lib-parser" Class
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" Bag (bagToList)

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Maybe
import qualified Data.HashSet as HashSet
import Data.List.Extra
import Data.List.Extended (spanMaybe)
import Data.Maybe
import qualified Data.Map.Strict as MS
import qualified Data.Set as Set
import qualified Data.Text as T

-- | Extract documentation in a dependency graph of modules.
extractDocs ::
    ExtractOptions
    -> Service.NotificationHandler
    -> Options
    -> [NormalizedFilePath]
    -> MaybeT IO [ModuleDoc]
extractDocs extractOpts diagsLogger ideOpts fp = do
    modules <- haddockParse diagsLogger ideOpts fp
    pure $ map mkModuleDocs modules

  where
    modDoc :: TypecheckedModule -> Maybe DocText
    modDoc
        = fmap (moduleDocToText . unLoc)
        . hsmodHaddockModHeader . unLoc
        . pm_parsed_source . tm_parsed_module

    mkModuleDocs :: Service.TcModuleResult -> ModuleDoc
    mkModuleDocs tmr =
        let tcmod = Service.tmrModule tmr
            ctx@DocCtx{..} = buildDocCtx extractOpts tcmod
            typeMap = MS.fromList $ mapMaybe (getTypeDocs ctx) dc_decls
            md_classes = mapMaybe (getClsDocs ctx) dc_decls

            interfaceInstanceMap = getInterfaceInstanceMap ctx dc_decls

            md_name = dc_modname
            md_anchor = Just (moduleAnchor md_name)
            md_descr = modDoc tcmod
            md_templates = getTemplateDocs ctx typeMap interfaceInstanceMap
            md_interfaces = getInterfaceDocs ctx typeMap interfaceInstanceMap
            md_functions = mapMaybe (getFctDocs ctx) dc_decls
            md_instances = map (getInstanceDocs ctx) dc_insts

            -- Type constructor docs without data types corresponding to
            -- templates, interfaces and choices
            adts
                = MS.elems . MS.withoutKeys typeMap . Set.unions
                $ dc_templates : dc_interfaces : MS.elems dc_choices

            md_adts = mapMaybe (filterTypeByExports md_name dc_exports) adts

        in ModuleDoc {..}

-- | This is equivalent to Haddock’s Haddock.Interface.Create.collectDocs
collectDocs :: [LHsDecl GhcPs] -> [DeclData]
collectDocs ds
    | (nextDocs, decl:ds') <- spanMaybe getNextOrPrevDoc ds
    , (prevDocs, ds'') <- spanMaybe getPrevDoc ds'
    = DeclData decl (joinDocs nextDocs prevDocs)
    : collectDocs ds''

    | otherwise
    = [] -- nothing to document
  where

    joinDocs :: [DocText] -> [DocText] -> Maybe DocText
    joinDocs nextDocs prevDocs =
        let docs = map unDocText (nextDocs ++ prevDocs)
        in if null docs
            then Nothing
            else Just . DocText . T.strip $ T.unlines docs

    getNextOrPrevDoc :: LHsDecl a -> Maybe DocText
    getNextOrPrevDoc = \case
        L _ (DocD _ (DocCommentNext str)) -> Just (docToText str)
        L _ (DocD _ (DocCommentPrev str)) -> Just (docToText str)
            -- technically this is a malformed doc, but we'll take it
        _ -> Nothing

    getPrevDoc :: LHsDecl a -> Maybe DocText
    getPrevDoc = \case
        L _ (DocD _ (DocCommentPrev str)) -> Just (docToText str)
        _ -> Nothing

buildDocCtx :: ExtractOptions -> TypecheckedModule -> DocCtx
buildDocCtx dc_extractOptions tcmod  =
    let parsedMod = tm_parsed_module tcmod
        checkedModInfo = tm_checked_module_info tcmod
        dc_ghcMod = ms_mod $ pm_mod_summary parsedMod
        dc_modname = getModulename dc_ghcMod
        dc_decls
            = collectDocs . hsmodDecls . unLoc
            . pm_parsed_source $ parsedMod
        (dc_templates, dc_interfaces, dc_choices) = getTemplateData parsedMod

        tythings = modInfoTyThings checkedModInfo
        dc_insts = modInfoInstances checkedModInfo

        dc_tycons = MS.fromList
            [ (typename, tycon)
            | ATyCon tycon <- tythings
            , let typename = Typename . packName . tyConName $ tycon
            ]

        dc_datacons = MS.fromList
            [ (conname, datacon)
            | AConLike (RealDataCon datacon) <- tythings
            , let conname = Typename . packName . dataConName $ datacon
            ]

        dc_ids = MS.fromList
            [ (fieldname, id)
            | AnId id <- tythings
            , let fieldname = Fieldname . packId $ id
            ]

        dc_exports = extractExports parsedMod

    in DocCtx {..}

-- | Parse and typecheck a module and its dependencies in Haddock mode
--   (retaining Doc declarations), and return the 'TcModuleResult's in
--   dependency order (top module last).
--
--   "Internal" modules are filtered out, they don't contribute to the docs.
--
--   Not using the cached file store, as it is expected to run stand-alone
--   invoked by a CLI tool.
haddockParse :: Service.NotificationHandler ->
                Options ->
                [NormalizedFilePath] ->
                MaybeT IO [Service.TcModuleResult]
haddockParse diagsLogger opts f = MaybeT $ do
  withDamlIdeState opts Logger.makeNopHandle diagsLogger $ \service -> do
      -- Discarding internal modules isn’t strictly necessary since we do not compile
      -- to Core. However, the errors that you get if you do accidentally compile to core
      -- which happens if you set them as files of interest and enable the ofInterestRule
      -- are so confusing that we filter it out anyway.
      nonInternal <- Service.runAction service $
          Service.discardInternalModules (optUnitId opts) f
      liftIO $ Service.setFilesOfInterest service (HashSet.fromList nonInternal)
      Service.runActionSync service $ runMaybeT $ do
          deps <- usesE' Service.GetDependencies f
          usesE' Service.TypeCheck $ nubOrd $ f ++ concatMap Service.transitiveModuleDeps deps
              -- We enable Opt_Haddock in the opts for daml-doc.
              --
          where
            usesE' k = fmap (map fst) . Service.usesE k

------------------------------------------------------------

-- | Extracts the set of interface instances declared in each (template or interface) type.
getInterfaceInstanceMap :: DocCtx -> [DeclData] -> MS.Map Typename (Set.Set InterfaceInstanceDoc)
getInterfaceInstanceMap ctx@DocCtx{..} decls =
    MS.fromListWith Set.union
        [ (parent, Set.singleton (InterfaceInstanceDoc interface template))
        | DeclData decl _ <- decls
        , name <- case unLoc decl of
            SigD _ (TypeSig _ (L _ n :_) _) -> [packRdrName n]
            _ -> []
        , Just _ <- [T.stripPrefix "_interface_instance_" name]
        , Just id <- [MS.lookup (Fieldname name) dc_ids]
        , TypeApp _ (Typename "InterfaceInstance")
            [ TypeApp _ parent []
            , interface
            , template
            ] <- [typeToType ctx $ idType id]
        ]

-- | Extracts the documentation of a function. Comments are either
--   adjacent to a type signature, or to the actual function definition. If
--   neither a comment nor a function type is in the source, we omit the
--   function.
getFctDocs :: DocCtx -> DeclData -> Maybe FunctionDoc
getFctDocs ctx@DocCtx{..} (DeclData decl docs) = do
    name <- case unLoc decl of
        SigD _ (TypeSig _ (L _ n :_) _) -> Just n
        ValD _ FunBind{..} | not (null docs) ->
            Just (unLoc fun_id)
            -- NB assuming we do _not_ have a type signature for the function in the
            -- pairs (otherwise we'll get a duplicate)
        _ ->
            Nothing

    let fct_name = Fieldname (packRdrName name)
    id <- MS.lookup fct_name dc_ids

    let ty = idType id
        fct_context = typeToContext ctx ty
        fct_type = typeToType ctx ty
        fct_anchor = Just $ functionAnchor dc_modname fct_name
        fct_descr = docs

    guard (exportsFunction dc_exports fct_name)
    guard (not $ "_choice$_" `T.isPrefixOf` packRdrName name)
    guard (not $ "_interface_instance_" `T.isPrefixOf` packRdrName name)
    guard (not $ "_requires$_" `T.isPrefixOf` packRdrName name)
    guard (not $ "_method_" `T.isPrefixOf` packRdrName name)
    guard (not $ "_view_" `T.isPrefixOf` packRdrName name)
    Just FunctionDoc {..}

getClsDocs :: DocCtx -> DeclData -> Maybe ClassDoc
getClsDocs ctx@DocCtx{..} (DeclData (L _ (TyClD _ ClassDecl{..})) tcdocs) = do
    let cl_name = Typename . packRdrName $ unLoc tcdLName
    tycon <- MS.lookup cl_name dc_tycons
    tycls <- tyConClass_maybe tycon
    let cl_anchor = Just $ classAnchor dc_modname cl_name
        cl_descr = tcdocs
        cl_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
        opMap = MS.fromList
            [ (Fieldname $ packId id, (id, dmInfoM))
            | (id, dmInfoM) <- classOpItems tycls ]
        cl_methods = concatMap (getMethodDocs opMap cl_anchor cl_name cl_args) subDecls
        cl_super = Context (map (typeToType ctx) (classSCTheta tycls))
        cl_instances = Nothing -- filled out later in 'distributeInstanceDocs'
    guard (exportsType dc_exports cl_name)
    Just ClassDoc {..}
  where
    -- All documentation of class members is stored in tcdDocs.
    -- To associate the docs with the correct member, we convert all members
    -- and docs to declarations, sort them by their location
    -- and then use collectDocs.
    -- This is the equivalent of Haddock’s Haddock.Interface.Create.classDecls.
    subDecls :: [DeclData]
    subDecls = collectDocs . sortOn getLoc $ decls
    decls = docs ++ defs ++ sigs ++ ats
    docs = map (fmap (DocD noExt)) tcdDocs
    defs = map (fmap (ValD noExt)) (bagToList tcdMeths)
    sigs = map (fmap (SigD noExt)) tcdSigs
    ats = map (fmap (TyClD noExt . FamDecl noExt)) tcdATs

    -- | Extract typeclass method docs from a subDecl. Notice that
    -- we may have to generate multiple docs from a single declaration,
    -- thanks to declarations of the form
    --
    -- @
    --     (+), (-) : t -> t -> t
    -- @
    --
    -- where multiple names are present. In that case we just duplicate
    -- the associated docs for each name that is defined.
    getMethodDocs ::
        MS.Map Fieldname ClassOpItem
        -> Maybe Anchor
        -> Typename
        -> [T.Text]
        -> DeclData
        -> [ClassMethodDoc]
    getMethodDocs opMap cl_anchor cl_name cl_args = \case
        DeclData (L _ (SigD _ (ClassOpSig _ cm_isDefault rdrNamesL _))) cm_descr ->
            flip mapMaybe rdrNamesL $ \rdrNameL -> do
                let cm_name = Fieldname . packRdrName . unLoc $ rdrNameL
                    cm_anchor = guard (not cm_isDefault) >>
                        Just (functionAnchor dc_modname cm_name)
                (id, dmInfoM) <- MS.lookup cm_name opMap

                let ghcType
                        | cm_isDefault = -- processing default method type sig
                            case dmInfoM of
                                Nothing ->
                                    error "getMethodDocs: expected default method to have associated default method info"
                                Just (_, VanillaDM) -> idType id
                                Just (_, GenericDM ty) -> ty
                        | otherwise = -- processing original method type sig
                            idType id
                    cm_type = typeToType ctx ghcType
                    cm_globalContext = typeToContext ctx ghcType
                    cm_localContext = dropMemberContext cl_anchor cl_args cm_globalContext
                guard (exportsField dc_exports cl_name cm_name)
                Just ClassMethodDoc{..}

        _ -> []

    -- | Remove the implied context from typeclass member functions.
    dropMemberContext :: Maybe Anchor -> [T.Text] -> DDoc.Context -> DDoc.Context
    dropMemberContext cl_anchor cl_args (Context xs) = Context $
        filter (not . matchesMemberContext cl_anchor cl_args) xs

    -- | Is this the implied context for member functions? We use an anchor
    -- for comparison because it is more accurate than typenames (which are
    -- generally susceptible to qualification and shadowing).
    matchesMemberContext :: Maybe Anchor -> [T.Text] -> DDoc.Type -> Bool
    matchesMemberContext cl_anchor cl_args ty =
        cl_anchor == getTypeAppAnchor ty &&
        Just [(Just (Typename arg), Just []) | arg <- cl_args] ==
            (map (\arg -> (getTypeAppName arg, getTypeAppArgs arg))
            <$> getTypeAppArgs ty)

getClsDocs _ _ = Nothing

unknownType :: DDoc.Type
unknownType = TypeApp Nothing (Typename "_") []

getTypeDocs :: DocCtx -> DeclData -> Maybe (Typename, ADTDoc)
getTypeDocs ctx@DocCtx{..} (DeclData (L _ (TyClD _ decl)) doc)
    | XTyClDecl{} <- decl =
        Nothing
    | ClassDecl{} <- decl =
        Nothing
    | FamDecl{}   <- decl =
        Nothing

    | SynDecl{..} <- decl = do
        let ad_name = Typename . packRdrName $ unLoc tcdLName
            ad_descr = doc
            ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
            ad_anchor = Just $ typeAnchor dc_modname ad_name
            ad_rhs = fromMaybe unknownType $ do
                tycon <- MS.lookup ad_name dc_tycons
                rhs <- synTyConRhs_maybe tycon
                Just (typeToType ctx rhs)
            ad_instances = Nothing -- filled out later in 'distributeInstanceDocs'
        Just (ad_name, TypeSynDoc {..})

    | DataDecl{..} <- decl = do
        let ad_name = Typename . packRdrName $ unLoc tcdLName
            ad_descr = doc
            ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
            ad_anchor = Just $ typeAnchor dc_modname ad_name
            ad_constrs = map constrDoc . dd_cons $ tcdDataDefn
            ad_instances = Nothing -- filled out later in 'distributeInstanceDocs'
        Just (ad_name, ADTDoc {..})
  where
    constrDoc :: LConDecl GhcPs -> ADTConstr
    constrDoc (L _ con) =
        let ac_name = Typename . packRdrName . unLoc $ con_name con
            ac_anchor = Just $ constrAnchor dc_modname ac_name
            ac_descr = fmap (docToText . unLoc) $ con_doc con
            ac_args =
                case MS.lookup ac_name dc_datacons of
                    Nothing ->
                        -- no type info available for args ... we can display "_" though
                        replicate
                            (length . hsConDeclArgTys . con_args $ con)
                            unknownType
                    Just datacon ->
                        map (typeToType ctx) (dataConOrigArgTys datacon)
        in case con_args con of
            PrefixCon _ -> PrefixC {..}
            InfixCon _ _ -> PrefixC {..} -- FIXME: should probably change this!
            RecCon (L _ fs) ->
                let ac_fields = mapMaybe fieldDoc (zip ac_args fs)
                in RecordC {..}

    fieldDoc :: (DDoc.Type, LConDeclField GhcPs) -> Maybe FieldDoc
    fieldDoc (fd_type, L _ ConDeclField{..}) = do
        let fd_name = Fieldname . T.concat . map (packFieldOcc . unLoc) $ cd_fld_names
            fd_anchor = Just $ functionAnchor dc_modname fd_name
            fd_descr = fmap (docToText . unLoc) cd_fld_doc
        Just FieldDoc{..}
    fieldDoc (_, L _ XConDeclField{}) = Nothing

getTypeDocs _ _other = Nothing
