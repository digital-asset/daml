-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ApplicativeDo       #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Visual
  ( execVisual
  ) where


import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.World as AST
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified DA.Pretty as DAP
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified Codec.Archive.Zip as ZIPArchive
import qualified Data.ByteString.Lazy as BSL
import Text.Dot
import qualified Data.ByteString as B
import Codec.Archive.Zip
import System.FilePath
import qualified Data.Map as M
import Data.Word
import qualified Data.HashMap.Strict as Map
import qualified Data.ByteString.Char8 as BS
import qualified Data.List.Split as DLS
import qualified Data.List as DL
import qualified Data.List.Extra as DE

data Action = ACreate (LF.Qualified LF.TypeConName)
            | AExercise (LF.Qualified LF.TypeConName) LF.ChoiceName deriving (Eq, Ord, Show )


startFromUpdate :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Update -> Set.Set Action
startFromUpdate seen world update = case update of
    LF.UPure _ e -> startFromExpr seen world e
    LF.UBind (LF.Binding _ e1) e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2
    LF.UCreate tpl e -> Set.singleton (ACreate tpl) `Set.union` startFromExpr seen world e
    LF.UExercise tpl chc e1 e2 e3 -> Set.singleton (AExercise tpl chc) `Set.union` startFromExpr seen world e1 `Set.union` maybe Set.empty (startFromExpr seen world) e2 `Set.union` startFromExpr seen world e3
    LF.UFetch _ ctIdEx -> startFromExpr seen world ctIdEx
    LF.UGetTime -> Set.empty
    LF.UEmbedExpr _ upEx -> startFromExpr seen world upEx
    LF.ULookupByKey _ -> Set.empty
    LF.UFetchByKey _ -> Set.empty

startFromExpr :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World  -> LF.Expr -> Set.Set Action
startFromExpr seen world e = case e of
    LF.EVar _ -> Set.empty
    LF.EVal ref->  case LF.lookupValue ref world of
        Right LF.DefValue{..}
            | ref `Set.member` seen  -> Set.empty
            | otherwise -> startFromExpr (Set.insert ref seen)  world dvalBody
        Left _ -> error "This should not happen"
    LF.EBuiltin _ -> Set.empty
    LF.ERecCon _ flds -> Set.unions $ map (\(_, exp) -> startFromExpr seen world exp) flds
    LF.ERecProj _ _ recEx -> startFromExpr seen world recEx
    LF.ETupleUpd _ recExpr recUpdate -> startFromExpr seen world recExpr `Set.union` startFromExpr seen world recUpdate
    LF.EVariantCon _ _ varg -> startFromExpr seen world varg
    LF.ETupleCon tcon -> Set.unions $ map (\(_, exp) -> startFromExpr seen world exp) tcon
    LF.ETupleProj _ tupExpr -> startFromExpr seen world tupExpr
    LF.ERecUpd _ _ recExpr recUpdate -> startFromExpr seen world recExpr `Set.union` startFromExpr seen world recUpdate
    LF.ETmApp tmExpr tmpArg -> startFromExpr seen world tmExpr `Set.union` startFromExpr seen world tmpArg
    LF.ETyApp tAppExpr _ -> startFromExpr seen world tAppExpr
    LF.ETmLam _ tmlB -> startFromExpr seen world tmlB
    LF.ETyLam _ lambdy -> startFromExpr seen world lambdy
    LF.ECase cas casel -> startFromExpr seen world cas `Set.union` Set.unions ( map ( startFromExpr seen world . LF.altExpr ) casel)
    LF.ELet (LF.Binding _ e1) e2 -> startFromExpr seen  world e1 `Set.union` startFromExpr seen world e2
    LF.ENil _ -> Set.empty
    LF.ECons _ consH consT -> startFromExpr seen world consH `Set.union` startFromExpr seen world consT
    LF.ESome _ smBdy -> startFromExpr seen world smBdy
    LF.ENone _ -> Set.empty
    LF.EUpdate upd -> startFromUpdate seen world upd
    LF.EScenario _ -> Set.empty
    LF.ELocation _ e1 -> startFromExpr seen world e1
    -- x -> Set.unions $ map startFromExpr $ children x

startFromChoice :: LF.World -> LF.TemplateChoice -> Set.Set Action
startFromChoice world chc = startFromExpr Set.empty world (LF.chcUpdate chc)

templatePossibleUpdates :: LF.World -> LF.Template -> Set.Set Action
templatePossibleUpdates world tpl = Set.unions $ map (startFromChoice world) (NM.toList (LF.tplChoices tpl))

moduleAndTemplates :: LF.World -> LF.Module -> [(LF.TypeConName, Set.Set Action)]
moduleAndTemplates world mod = retTypess
    where
        templates = NM.toList $ LF.moduleTemplates mod
        retTypess = map (\t-> (LF.tplTypeCon t, templatePossibleUpdates world t )) templates


dalfBytesToPakage :: BSL.ByteString -> (LF.PackageId, LF.Package)
dalfBytesToPakage bytes = case Archive.decodeArchive $ BSL.toStrict bytes of
    Right a -> a
    Left err -> error (show err)

darToWorld :: ManifestData -> LF.Package -> LF.World
darToWorld manifest pkg = AST.initWorldSelf pkgs pkg
    where
        pkgs = map dalfBytesToPakage (dalfsCotent manifest)


templateInAction :: Action -> LF.TypeConName
templateInAction (ACreate  (LF.Qualified _ _ tpl) ) = tpl
templateInAction (AExercise  (LF.Qualified _ _ tpl) _ ) = tpl

srcLabel :: (LF.TypeConName, Set.Set Action) -> [(String, String)]
srcLabel (tc, _) = [ ("shape","none"),("label",DAP.renderPretty tc) ]

templatePairs :: (LF.TypeConName, Set.Set Action) -> (LF.TypeConName , (LF.TypeConName , Set.Set Action))
templatePairs (tc, actions) = (tc , (tc, actions))

actionsForTemplate :: (LF.TypeConName, Set.Set Action) -> [LF.TypeConName]
actionsForTemplate (_tplCon, actions) = Set.elems $ Set.map templateInAction actions

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x


-- | 'netlistGraph' generates a simple graph from a netlist.
-- The default implementation does the edeges other way round. The change is on # 143
netlistGraph' :: (Ord a)
          => (b -> [(String,String)])   -- ^ Attributes for each node
          -> (b -> [a])                 -- ^ Out edges leaving each node
          -> [(a,b)]                    -- ^ The netlist
          -> Dot ()
netlistGraph' attrFn outFn assocs = do
    let nodes = Set.fromList [a | (a, _) <- assocs]
    let outs = Set.fromList [o | (_, b) <- assocs, o <- outFn b]
    nodeTab <- sequence
                [do nd <- node (attrFn b)
                    return (a, nd)
                | (a, b) <- assocs]
    otherTab <- sequence
               [do nd <- node []
                   return (o, nd)
                | o <- Set.toList outs, o `Set.notMember` nodes]
    let fm = M.fromList (nodeTab ++ otherTab)
    sequence_
        [(fm M.! dst) .->. (fm M.! src) | (dst, b) <- assocs,
        src <- outFn b]



data Manifest = Manifest { mainDalf :: FilePath , dalfs :: [FilePath] } deriving (Show)
data ManifestData = ManifestData { mainDalfContent :: BSL.ByteString , dalfsCotent :: [BSL.ByteString] } deriving (Show)

charToWord8 :: Char -> Word8
charToWord8 = toEnum . fromEnum

lineToKeyValue :: String -> (String, String)
lineToKeyValue line = case DE.splitOn ":" line of
    [l, r] -> (DE.trim l , DE.trim r)
    _ -> ("malformed", "malformed")

manifestMapToManifest :: Map.HashMap String String -> Manifest
manifestMapToManifest hash = Manifest mainDalf dependDalfs
    where
        mainDalf = Map.lookupDefault "unknown" "Main-Dalf" hash
        dependDalfs = map DE.trim $ DL.delete mainDalf (DLS.splitOn "," (Map.lookupDefault "unknown" "Dalfs" hash))

manifestDataFromDar :: Archive -> Manifest -> ManifestData
manifestDataFromDar archive manifest = ManifestData manifestDalfByte dependencyDalfBytes
    where
        manifestDalfByte = head [fromEntry e | e <- zEntries archive, ".dalf" `isExtensionOf` eRelativePath e  && eRelativePath e  == mainDalf manifest]
        dependencyDalfBytes = [fromEntry e | e <- zEntries archive, ".dalf" `isExtensionOf` eRelativePath e  && DL.elem (DE.trim (eRelativePath e))  (dalfs manifest)]

manifestFromDar :: Archive -> ManifestData
manifestFromDar dar =  manifestDataFromDar dar manifest
    where
        manifestEntry = head [fromEntry e | e <- zEntries dar, ".MF" `isExtensionOf` eRelativePath e]
        lines = BSL.split (charToWord8 '\n') manifestEntry
        linesStr = map (BS.unpack . BSL.toStrict) lines
        manifest = manifestMapToManifest $ Map.fromList $ map lineToKeyValue (filter (\a -> a /= "" ) linesStr)


execVisual :: FilePath -> IO ()
execVisual darFilePath = do
    darBytes <- B.readFile darFilePath
    let manifestData = manifestFromDar $ ZIPArchive.toArchive (BSL.fromStrict darBytes)
    (_, lfPkg) <- errorOnLeft "Cannot decode package" $ Archive.decodeArchive (BSL.toStrict (mainDalfContent manifestData) )
    let modules = NM.toList $ LF.packageModules lfPkg
        world = darToWorld manifestData lfPkg
        res = concatMap (moduleAndTemplates world) modules
        actionEdges = map templatePairs res
    putStrLn $ showDot $ do
        netlistGraph' srcLabel actionsForTemplate actionEdges


