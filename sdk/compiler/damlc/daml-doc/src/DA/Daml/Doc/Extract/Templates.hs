-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.Templates
    ( getTemplateDocs
    , getTemplateData
    , getInstanceDocs
    , getInterfaceDocs
    , getTemplateMaps
    , TemplateMaps (..)
    ) where

import DA.Daml.Doc.Types as DDoc
import DA.Daml.Doc.Extract.Types
import DA.Daml.Doc.Extract.Util
import DA.Daml.Doc.Extract.TypeExpr

import Control.Applicative ((<|>))
import qualified Data.Map.Strict as MS
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Tuple.Extra (second)
import qualified Data.Set as Set
import qualified Data.Text as T

import "ghc-lib" GHC
import "ghc-lib-parser" Bag (bagToList)
import "ghc-lib-parser" CoreSyn (isOrphan)
import "ghc-lib-parser" Id
import "ghc-lib-parser" InstEnv
import "ghc-lib-parser" Name
import "ghc-lib-parser" DynFlags (unsafeGlobalDynFlags)
import "ghc-lib-parser" Outputable (ppr, showSDocOneLine)
import "ghc-lib-parser" Var (varType)

-- import Debug.Trace

-- | Build template docs up from ADT and class docs.
getTemplateDocs ::
    DocCtx
    -> MS.Map Typename ADTDoc
      -- ^ maps type names to their ADT docs
    -> MS.Map Typename (Set.Set InterfaceInstanceDoc)
      -- ^ maps type names to the interface instances contained in their declaration.
    -> TemplateMaps
    -> [TemplateDoc]
getTemplateDocs DocCtx{..} typeMap interfaceInstanceMap templateMaps =
    map mkTemplateDoc $ Set.toList dc_templates
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkTemplateDoc :: Typename -> TemplateDoc
    mkTemplateDoc name = TemplateDoc
      { td_anchor = ad_anchor tmplADT
      , td_name = ad_name tmplADT
      , td_descr = ad_descr tmplADT
      , td_signatory = MS.lookup name $ signatoryMap templateMaps 
      , td_payload = getFields tmplADT
      -- assumes exactly one record constructor (syntactic, template syntax)
      , td_choices = map (mkChoiceDoc typeMap templateMaps name) choices
      , td_interfaceInstances =
          Set.toList (MS.findWithDefault mempty name interfaceInstanceMap)
     }
      where
        tmplADT = asADT typeMap name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices


-- | Build interface docs up from class docs.
getInterfaceDocs :: DocCtx
    -> MS.Map Typename ADTDoc
        -- ^ maps type names to their ADT docs
    -> MS.Map Typename (Set.Set InterfaceInstanceDoc)
        -- ^ maps type names to the interface instances contained in their declaration.
    -> TemplateMaps
    -> [InterfaceDoc]
getInterfaceDocs DocCtx{..} typeMap interfaceInstanceMap templateMaps =
    map mkInterfaceDoc $ Set.toList dc_interfaces
  where
    -- Here we replace the signatories of all interfaces with an informative message
    -- Otherwise it is always `GHC.Types.primitive @"ESignatoryInterface"` and will
    -- be used for the Archive controller
    updatedTemplateMaps :: Typename -> TemplateMaps
    updatedTemplateMaps name = templateMaps {signatoryMap = MS.singleton name ["Signatories of implementing template"]}
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkInterfaceDoc :: Typename -> InterfaceDoc
    mkInterfaceDoc name = InterfaceDoc
      { if_anchor = ad_anchor ifADT
      , if_name = ad_name ifADT
      , if_descr = ad_descr ifADT
      -- TODO Should replace signatories map here with `const "<Instance templates signatories>"`
      , if_choices = map (mkChoiceDoc typeMap (updatedTemplateMaps name) name) choices
      , if_methods = [] -- filled by distributeInstanceDocs
      , if_interfaceInstances =
          Set.toList (MS.findWithDefault mempty name interfaceInstanceMap)
      , if_viewtype = Nothing -- filled by distributeInstanceDocs
      }
      where
        ifADT = asADT typeMap name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices

-- | Extracts all names of templates and interfaces defined in a module,
-- and a map of template/interface names to its set of choices
getTemplateData :: ParsedModule ->
    ( Set.Set Typename
    , Set.Set Typename
    , MS.Map Typename (Set.Set Typename) )
getTemplateData ParsedModule{..} =
  let
    dataDs    = mapMaybe (isDataDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    instDs    = mapMaybe (isInstDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    templates = mapMaybe isTemplate dataDs
    interfaces = mapMaybe isInterface dataDs
    choiceMap = MS.fromListWith (<>) $
                map (second Set.singleton) $
                mapMaybe isChoice instDs
  in
    (Set.fromList templates, Set.fromList interfaces, choiceMap)
    where
      isInstDecl (InstD _ (ClsInstD _ i)) = Just i
      isInstDecl _ = Nothing
      isDataDecl (TyClD _ d@DataDecl{}) = Just d
      isDataDecl _ = Nothing


-- | If the given instance declaration is declaring a template instance, return
--   its name (IdP). Used to build the set of templates declared in a module.
isTemplate :: TyClDecl GhcPs -> Maybe Typename
isTemplate = hasGhcTypesConstraint "DamlTemplate"

isInterface :: TyClDecl GhcPs -> Maybe Typename
isInterface = hasGhcTypesConstraint "DamlInterface"

hasGhcTypesConstraint :: String -> TyClDecl GhcPs -> Maybe Typename
hasGhcTypesConstraint c decl
  | DataDecl {tcdLName, tcdDataDefn} <- decl
  , HsDataDefn {dd_ctxt} <- tcdDataDefn
  , L _ [L _ (HsTyVar _ _ (L _ (Qual mod cst)))] <- dd_ctxt
  , moduleNameString mod == "GHC.Types"
  , occNameString cst == c = Just $ Typename $ packRdrName $ unLoc tcdLName

  | otherwise = Nothing

-- | If the given instance declaration is declaring a template/interface choice instance,
--   return template/interface and choice name (IdP). Used to build the set of choices
--   per template/interface declared in a module.
isChoice :: ClsInstDecl GhcPs -> Maybe (Typename, Typename)
isChoice (XClsInstDecl _) = Nothing
isChoice ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  = isChoiceTy ty

isChoiceTy :: HsType GhcPs -> Maybe (Typename, Typename)
isChoiceTy ty
  | HsAppTy _ (L _ cApp1) (L _ _cArgs) <- ty
  , HsAppTy _ (L _ cApp2) cName <- cApp1
  , HsAppTy _ (L _ choice) cTmpl <- cApp2
  , HsTyVar _ _ (L _ choiceClass) <- choice
  , Just (L _ choiceName) <- hsTyGetAppHead_maybe cName
  , Just (L _ tmplName) <- hsTyGetAppHead_maybe cTmpl
  , Qual classModule classOcc <- choiceClass
  , moduleNameString classModule == "DA.Internal.Desugar"
  , occNameString classOcc == "HasExercise"
  = Just (Typename . packRdrName $ tmplName, Typename . packRdrName $ choiceName)

  | otherwise = Nothing

data TemplateMaps = TemplateMaps
  { choiceTypeMap :: MS.Map Typename DDoc.Type
  -- ^ maps choice names to their return types
  , signatoryMap :: MS.Map Typename [String]
  -- ^ maps template names to their signatory body stringified
  , choiceControllerMap :: MS.Map (Typename, Typename) [String]
  -- ^ maps choice names to their controller body stringified (excluding Archive)
  }

getTemplateMaps :: DocCtx -> TemplateMaps
getTemplateMaps ctx@DocCtx{..} =
  TemplateMaps
    (getChoiceTypeMap ctx dc_insts)
    (getSignatoryMap dc_decls)
    (getChoiceControllerMap dc_decls)

-- | Extracts the return types of choices by looking at the HasExercise
--   instances. Note that we expect and accept key clashes for `Archive`
--   but this is acceptable, as choice return types are tied only to the
--   Choice not the Choice + Template together.
getChoiceTypeMap :: DocCtx -> [ClsInst] -> MS.Map Typename DDoc.Type
getChoiceTypeMap ctx = MS.fromList . mapMaybe (isChoiceInst ctx)

isChoiceInst :: DocCtx -> ClsInst -> Maybe (Typename, DDoc.Type)
isChoiceInst ctx inst
  | instName <- is_cls_nm inst
  , "HasExercise" <- occNameString . occName $ instName
  , Just mName <- moduleName <$> nameModule_maybe instName
  , mkModuleName "DA.Internal.Template.Functions" == mName
  , [_, choiceType, retType] <- is_tys inst
  , TypeApp _ choiceName _ <- typeToType ctx choiceType
  = Just (choiceName, typeToType ctx retType)

  | otherwise = Nothing

hsExprToString :: HsExpr GhcPs -> String
hsExprToString = showSDocOneLine unsafeGlobalDynFlags . ppr

isSignatoryTy :: HsType GhcPs -> Maybe Typename
isSignatoryTy ty
  | HsAppTy _ (L _ signatory) template <- ty
  , Just (L _ templateName) <- hsTyGetAppHead_maybe template
  , HsTyVar _ _ (L _ signatoryClass) <- signatory
  , Qual signatoryModule signatoryOcc <- signatoryClass
  , moduleNameString signatoryModule == "DA.Internal.Desugar"
  , occNameString signatoryOcc == "HasSignatory"
  = Just $ Typename $ packRdrName templateName

  | otherwise = Nothing

getSignatoriesBody :: DeclData -> Maybe (Typename, [String])
getSignatoriesBody decl
  | DeclData (L _ (InstD _ (ClsInstD _ inst))) _ <- decl
  , HsIB _ (L _ t) <- cid_poly_ty inst
  , Just templateName <- isSignatoryTy t
  , [L _ (FunBind _ _ matchGroup _ _)] <- bagToList $ cid_binds inst
  , Just (body, _) <- unMatchGroup matchGroup
  , bodyStr <- hsExprToString <$> reverse (unParties body)
  = Just (templateName, bodyStr)

  | otherwise = Nothing

-- | Extracts the template name and signature implementation for `DA.Internal.Desugar.HasSignatory` instances
getSignatoryMap :: [DeclData] -> MS.Map Typename [String]
getSignatoryMap = MS.fromList . mapMaybe getSignatoriesBody

stripDesugarApplication :: String -> HsExpr GhcPs -> Maybe (HsExpr GhcPs)
stripDesugarApplication name e
  | HsApp _ (L _ (HsVar _ (L _ f))) (L _ arg) <- e
  , Qual fModule fOcc <- f
  , moduleNameString fModule == "DA.Internal.Desugar"
  , occNameString fOcc == name
  = Just $ dropBrackets arg

  | otherwise = Nothing

-- | Given a value encoded with the `parties` rule in the grammar,
-- Strip the additional applications used to make it compile and give back the "original" form
-- That is, check for the `concat []` optional wrapper, remove that. And remove all the `toParties` calls
unParties :: HsExpr GhcPs -> [HsExpr GhcPs]
unParties e
  | Just partiesHsExpr <- stripDesugarApplication "concat" e
  , ExplicitList _ _ partyHsExprs <- partiesHsExpr
  = unParty . unLoc <$> partyHsExprs
  | otherwise = [unParty e]

unParty :: HsExpr GhcPs -> HsExpr GhcPs
unParty e = fromMaybe e $ stripDesugarApplication "toParties" e

unMatchGroup :: MatchGroup GhcPs (LHsExpr GhcPs) -> Maybe (HsExpr GhcPs, [Pat GhcPs])
unMatchGroup matchGroup
  | MG _ (L _ matches) _ <- matchGroup
  , [L _ (Match _ _ pats _ (GRHSs _ [L _ (GRHS _ _ (L _ body))] _))] <- matches
  = Just (body, pats)

  | otherwise = Nothing

dropBrackets :: HsExpr GhcPs -> HsExpr GhcPs
dropBrackets (HsPar _ (L _ body)) = body
dropBrackets e = e

-- | Mapping from template and choice type to controller stringified
getChoiceControllerMap :: [DeclData] -> MS.Map (Typename, Typename) [String]
getChoiceControllerMap = MS.fromList . mapMaybe getChoiceControllerData

getChoiceControllerData :: DeclData -> Maybe ((Typename, Typename), [String])
getChoiceControllerData decl
  | DeclData (L _ (ValD _ (FunBind _ (L _ name) matchGroup _ _))) _ <- decl
  -- Check the name of the def starts with _choice$_
  , ("_choice$_", name) <- T.splitAt 9 $ packRdrName name
  , [templateName, choiceName] <- Typename <$> T.split (=='$') name
  -- Extract the type and body of the definition (as choiceType :: HsType and body :: hsExpr)
  , Just (body, _) <- unMatchGroup matchGroup
  , ExplicitTuple _ [_, L _ (Present _ (L _ controllerExpr)), _, _, _] _ <- body
  , Just controllers <- unControllerExpr controllerExpr
  , controllersStr <- hsExprToString <$> unParties controllers
  = Just ((templateName, choiceName), controllersStr)

  | otherwise = Nothing

-- | One of either
-- \ x y -> Body
-- \ x -> bypassReduceLambda \y -> Body
-- Body must be `let _ in let _ in e`
-- Return Nothing for detected Archives, as they will be replaced with signatories
unControllerExpr :: HsExpr GhcPs -> Maybe (HsExpr GhcPs)
unControllerExpr e
  -- \x y -> Body
  | HsLam _ matchGroup <- e
  , Just (body, [_, sndPat]) <- unMatchGroup matchGroup
  = case sndPat of
      WildPat _ -> Nothing -- If `y` is _, we're in an Archive.
      _ -> unLets1 body

  -- \x -> bypassReduceLambda $ \y -> Body
  | HsLam _ matchGroup <- e
  , Just (body, [_]) <- unMatchGroup matchGroup
  , Just innerLambda <- stripDesugarApplication "bypassReduceLambda" body
  , HsLam _ matchGroup <- innerLambda
  , Just (innerBody, _) <- unMatchGroup matchGroup
  = unLets1 innerBody

  | otherwise = Nothing
  where
    -- Drop as many let binds as possible, at least one (will be stopped by the application of toParty or concat)
    -- We cannot drop a fixed number of binds, as the number of binds depends on the let body of the template
    unLets1 :: HsExpr GhcPs -> Maybe (HsExpr GhcPs)
    unLets1 (HsLet _ _ (L _ body)) = Just $ unLets body
    unLets1 _ = Nothing
    unLets :: HsExpr GhcPs -> HsExpr GhcPs
    unLets (HsLet _ _ (L _ body)) = unLets body
    unLets body = body
      

-- | Get (normal) typeclass instances data.
getInstanceDocs :: DocCtx -> ClsInst -> InstanceDoc
getInstanceDocs ctx@DocCtx{dc_decls} ClsInst{..} =
  let ty = varType is_dfun
      srcSpan = getLoc $ idName is_dfun
      modname = Modulename $ T.pack $ moduleNameString $ moduleName $ nameModule is_cls_nm
      instDocMap = MS.fromList [(l, doc) | (DeclData (L l (InstD _x _i)) (Just doc)) <- dc_decls]
  in InstanceDoc
      { id_context = typeToContext ctx ty
      , id_module = modname
      , id_type = typeToType ctx ty
      , id_isOrphan = isOrphan is_orphan
      , id_descr = MS.lookup srcSpan instDocMap
      }

-- Utilities common to templates and interfaces
-----------------------------------------------

-- | Create an ADT from a Typename
asADT :: MS.Map Typename ADTDoc -> Typename -> ADTDoc
asADT typeMap n = fromMaybe dummyDT $ MS.lookup n typeMap
  where
    dummyDT =
      ADTDoc
        { ad_anchor = Nothing
        , ad_name = dummyName n
        , ad_descr = Nothing
        , ad_args = []
        , ad_constrs = []
        , ad_instances = Nothing
        }
    dummyName (Typename "Archive") = Typename "Archive"
    dummyName (Typename t) = Typename $ "External:" <> t

-- | Assuming one constructor (record or prefix), extract the fields, if any.  For choices without
-- arguments, GHC returns a prefix constructor, so we need to cater for this case specially.
getFields :: ADTDoc -> [FieldDoc]
getFields adt =
  case ad_constrs adt of
    [PrefixC {}] -> []
    [RecordC {ac_fields = fields}] -> fields
    [] -> [] -- catching the dummy case here, see above
    _other -> error "getFields: found multiple constructors"

mkChoiceDoc :: MS.Map Typename ADTDoc -> TemplateMaps -> Typename -> Typename -> ChoiceDoc
mkChoiceDoc typeMap templateMaps templateName choiceName =
  ChoiceDoc
    { cd_anchor = ad_anchor choiceADT
    , cd_name = ad_name choiceADT
    , cd_descr = ad_descr choiceADT
    -- Attempt controller lookup, fallback to signatories
    , cd_controller =
        MS.lookup (templateName, choiceName) (choiceControllerMap templateMaps) 
          <|> MS.lookup templateName (signatoryMap templateMaps)
    -- assumes exactly one constructor (syntactic in the template syntax), or
    -- uses a dummy value otherwise.
    , cd_fields = getFields choiceADT
    , cd_type = fromMaybe (TypeApp Nothing (Typename "UnknownType") []) $ MS.lookup choiceName $ choiceTypeMap templateMaps
    }
  where
    choiceADT = asADT typeMap choiceName
