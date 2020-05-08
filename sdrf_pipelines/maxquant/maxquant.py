# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 09:46:14 2020

@author: ChengXin
"""
import pandas as pd
import re
from xml.dom.minidom import *



class Maxquant():
    
    def __init__(self) -> None:
        super().__init__()
        self.warnings = dict()
        
    def maxquant_ify_mods(self, sdrf_mods):
        oms_mods = list()
        
        for m in sdrf_mods:
            if "AC=UNIMOD" not in m and "AC=Unimod" not in m:
                raise Exception("only UNIMOD modifications supported. " + m)

            name = re.search("NT=(.+?)(;|$)", m).group(1)
            name = name.capitalize()

            # workaround for missing PP in some sdrf TODO: fix in sdrf spec?
            if re.search("PP=(.+?)[;$]", m) is None:
                pp = "Anywhere"
            else:
                pp = re.search("PP=(.+?)(;|$)", m).group(1)  # one of [Anywhere, Protein N-term, Protein C-term, Any N-term, Any C-term

            if re.search("TA=(.+?)(;|$)", m) is None:  # TODO: missing in sdrf.
                warning_message = "Warning no TA= specified. Setting to N-term or C-term if possible."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                if "C-term" in pp:
                    ta = "C-term"
                elif "N-term" in pp:
                    ta = "N-term"
                else:
                    warning_message = "Reassignment not possible. Skipping."
                    # print(warning_message + " "+ m)
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                    pass
            else:
                ta = re.search("TA=(.+?)(;|$)", m).group(1)  # target amino-acid
            aa = ta.split(",")  # multiply target site e.g., S,T,Y including potentially termini "C-term"

            if pp == "Protein N-term" or pp == "Protein C-term":
                for a in aa:
                    if a == "C-term" or a == "N-term":  # no site specificity
                        oms_mods.append(name + " (" + pp + ")")  # any Protein N/C-term
                    else:
                        oms_mods.append(name + " (" + pp + " " + a + ")")  # specific Protein N/C-term
            elif pp == "Any N-term" or pp == "Any C-term":
                pp = pp.replace("Any ", "")  # in OpenMS we just use N-term and C-term
                for a in aa:
                    if a == "C-term" or aa == "N-term":  # no site specificity
                        oms_mods.append(name + " (" + pp + ")")  # any N/C-term
                    else:
                        oms_mods.append(name + " (" + pp + " " + a + ")")  # specific N/C-term
            else:  # Anywhere in the peptide
                for a in aa:
                    oms_mods.append(name + " (" + a + ")")  # specific site in peptide

        return ",".join(oms_mods)
    
    def maxquant_convert(self,sdrf_file, fastaFilePath, matchBetweenRuns, peptideFDR, proteinFDR, tempFolder, raw_Folder,numThreads,output_path):
        print('PROCESSING: ' + sdrf_file + '"')
        sdrf = pd.read_csv(sdrf_file, sep ='\t')
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)  # convert column names to lower-case

        # map filename to tuple of [fixed, variable] mods
        mod_cols = [c for ind, c in enumerate(sdrf) if
                    c.startswith('comment[modification parameters')]  # columns with modification parameters
                    
        # get factor columns (except constant ones)
        factor_cols = [c for ind, c in enumerate(sdrf) if c.startswith('factor value[') and len(sdrf[c].unique()) > 1]
        
        # get characteristics columns (except constant ones)
        characteristics_cols = [c for ind, c in enumerate(sdrf) if
                                c.startswith('characteristics[') and len(sdrf[c].unique()) > 1]
        
        # remove characteristics columns already present as factor
        redundant_characteristics_cols = set()
        for c in characteristics_cols:
            c_col = sdrf[c]  # select characteristics column
            for f in factor_cols:  # Iterate over all factor columns
                f_col = sdrf[f]  # select factor column
                if c_col.equals(f_col):
                    redundant_characteristics_cols.add(c)
        characteristics_cols = [x for x in characteristics_cols if x not in redundant_characteristics_cols]

        enzy_cols = [c for ind, c in enumerate(sdrf) if
                    c.startswith('comment[cleavage agent details]')]
        
            
        file2mods = dict()
        file2pctol = dict()
        file2pctolunit = dict()
        file2fragtol = dict()
        file2fragtolunit = dict()
        file2diss = dict()
        file2enzyme = dict()
        file2fraction = dict()
        file2label = dict()
        file2source = dict()
        source_name_list = list()
        source_name2n_reps = dict()
        file2technical_rep = dict()
        file2instrument = dict()

        for index, row in sdrf.iterrows():
            all_enzy = list(row[enzy_cols])
            
            ## extract mods
            all_mods = list(row[mod_cols])
            # print(all_mods)
            var_mods = [m for m in all_mods if 'MT=variable' in m or 'MT=Variable' in m]  # workaround for capitalization
            var_mods.sort()
            fixed_mods = [m for m in all_mods if 'MT=fixed' in m or 'MT=Fixed' in m]  # workaround for capitalization
            fixed_mods.sort()
            
            raw = row['comment[data file]']
            fixed_mods_string = ""
            if fixed_mods is not None:
                fixed_mods_string = self.maxquant_ify_mods(fixed_mods)

            variable_mods_string = ""
            if var_mods is not None:
                variable_mods_string = self.maxquant_ify_mods(var_mods)

            file2mods[raw] = (fixed_mods_string, variable_mods_string)
            source_name = row['source name']
            file2source[raw] = source_name
            if not source_name in source_name_list:
                source_name_list.append(source_name)
                
            file2instrument[raw] = row['comment[instrument]']
            
            if 'comment[precursor mass tolerance]' in row:
                pc_tol_str = row['comment[precursor mass tolerance]']
                if "ppm" in pc_tol_str or "Da" in pc_tol_str:
                    pc_tmp = pc_tol_str.split(" ")
                    file2pctol[raw] = pc_tmp[0]
                    file2pctolunit[raw] = pc_tmp[1]
                else:
                    warning_message = "Invalid precursor mass tolerance set. Assuming 10 ppm."
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                    file2pctol[raw] = "10"
                    file2pctolunit[raw] = "ppm"
            else:
                warning_message = "No precursor mass tolerance set. Assuming 10 ppm."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                file2pctol[raw] = "10"
                file2pctolunit[raw] = "ppm"

            if 'comment[fragment mass tolerance]' in row:
                f_tol_str = row['comment[fragment mass tolerance]']
                f_tol_str.replace("PPM", "ppm")  # workaround
                if "ppm" in f_tol_str or "Da" in f_tol_str:
                    f_tmp = f_tol_str.split(" ")
                    file2fragtol[raw] = f_tmp[0]
                    file2fragtolunit[raw] = f_tmp[1]
                else:
                    warning_message = "Invalid fragment mass tolerance set. Assuming 20 ppm."
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                    file2fragtol[raw] = "20"
                    file2fragtolunit[raw] = "ppm"
            else:
                warning_message = "No fragment mass tolerance set. Assuming 20 ppm."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                file2fragtol[raw] = "20"
                file2fragtolunit[raw] = "ppm"

            if 'comment[dissociation method]' in row:
                if row['comment[dissociation method]'] == 'not available':
                    file2diss[raw] = 'HCD'
                else:
                    diss_method = re.search("NT=(.+?)(;|$)", row['comment[dissociation method]']).group(1)
                    file2diss[raw] = diss_method.upper()
            else:
                warning_message = "No dissociation method provided. Assuming HCD."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                file2diss[raw] = 'HCD'

            if 'comment[technical replicate]' in row:
                technical_replicate = str(row['comment[technical replicate]'])
                if "not available" in technical_replicate:
                    file2technical_rep[raw] = "1"
                else:
                    file2technical_rep[raw] = technical_replicate
            else:
                file2technical_rep[raw] = "1"

            # store highest replicate number for this source name
            if source_name in source_name2n_reps:
                source_name2n_reps[source_name] = max(int(source_name2n_reps[source_name]), int(file2technical_rep[raw]))
            else:
                source_name2n_reps[source_name] = int(file2technical_rep[raw])
            
            e_list = []
            for e in all_enzy:
                enzyme = re.search("NT=(.+?)(;|$)", e).group(1)
                enzyme = enzyme.capitalize()
                if "Trypsin/p" in enzyme:  # workaround
                    enzyme = "Trypsin/P"
                e_list.append(enzyme)
            file2enzyme[raw] = e_list
            #print(enzyme)
            
            if 'comment[fraction identifier]' in row:
                fraction = str(row['comment[fraction identifier]'])
                if "not available" in fraction:
                    file2fraction[raw] = 0
                else:
                    file2fraction[raw] = fraction
            else:
                file2fraction[raw] = 0
             
            #Temporary support label free
            if "not  available" in row['comment[label]']:
                label = 'label free sample'
            else:
                try:
                    label = re.search("NT=(.+?)(;|$)", row['comment[label]']).group(1)

                except:
                    if row['comment[label]'] == 'iBAQ':
                        label = 'iBAQ'

                
            file2label[raw] = label
            
#        print(file2mods)
#        print(file2enzyme)
        
        #create maxquant parameters xml file
        doc=Document()
        
        #create default textnode:Empty
        Empty_text = doc.createTextNode('')
        #创建一个根节点
        root=doc.createElement('MaxQuantParams')
        root.setAttribute('xmlns:xsd',"http://www.w3.org/2001/XMLSchema")
        root.setAttribute('xmlns:xsi',"http://www.w3.org/2001/XMLSchema-instance")
        doc.appendChild(root)
        
        #create fastaFiles subnode
        fastaFiles = doc.createElement('fastaFiles')
        FastaFileInfo = doc.createElement('FastaFileInfo')
        fastaFilePath_node = doc.createElement('fastaFilePath')
        fastaFilePath_node.appendChild(doc.createTextNode(fastaFilePath))
        identifierParseRule = doc.createElement('identifierParseRule')
        identifierParseRule.appendChild(doc.createTextNode('>([^\s]*)'))   # will be improved
        descriptionParseRule = doc.createElement('descriptionParseRule')
        descriptionParseRule.appendChild(doc.createTextNode('>(.*)'))
        taxonomyParseRule = doc.createElement('taxonomyParseRule')
        taxonomyParseRule.appendChild(doc.createTextNode(''))
        variationParseRule = doc.createElement('variationParseRule')
        variationParseRule.appendChild(doc.createTextNode(''))
        modificationParseRule = doc.createElement('modificationParseRule')
        modificationParseRule.appendChild(doc.createTextNode(''))
        taxonomyId = doc.createElement('taxonomyId')
        taxonomyId.appendChild(doc.createTextNode(''))
        FastaFileInfo.appendChild(fastaFilePath_node)
        FastaFileInfo.appendChild(identifierParseRule)
        FastaFileInfo.appendChild(descriptionParseRule)
        FastaFileInfo.appendChild(taxonomyParseRule)
        FastaFileInfo.appendChild(variationParseRule)
        FastaFileInfo.appendChild(modificationParseRule)
        FastaFileInfo.appendChild(taxonomyId)
        fastaFiles.appendChild(FastaFileInfo)
        root.appendChild(fastaFiles)
        
        #create fastaFilesProteogenomics subnode
        fastaFilesProteogenomics = doc.createElement('fastaFilesProteogenomics')
        fastaFilesProteogenomics.appendChild(Empty_text)
        root.appendChild(fastaFilesProteogenomics)
        
        #create fastaFilesFirstSearch subnode
        fastaFilesFirstSearch = doc.createElement('fastaFilesFirstSearch')
        fastaFilesFirstSearch.appendChild(doc.createTextNode(''))
        root.appendChild(fastaFilesFirstSearch)
        
        #create fixedSearchFolder subnode
        fixedSearchFolder = doc.createElement('fixedSearchFolder')
        fixedSearchFolder.appendChild(doc.createTextNode(''))
        root.appendChild(fixedSearchFolder)
        
        #create andromedaCacheSize subnode
        andromedaCacheSize = doc.createElement('andromedaCacheSize')
        andromedaCacheSize.appendChild(doc.createTextNode('350000'))#default value
        root.appendChild(andromedaCacheSize)
        
        #create advancedRatios subnode
        advancedRatios = doc.createElement('advancedRatios')
        advancedRatios.appendChild(doc.createTextNode('True'))
        root.appendChild(advancedRatios)
        
        #create pvalThres subnode
        pvalThres = doc.createElement('pvalThres')
        pvalThres.appendChild(doc.createTextNode('0.005'))
        root.appendChild(pvalThres)
        
        #create neucodeRatioBasedQuantification subnode
        neucodeRatioBasedQuantification = doc.createElement('neucodeRatioBasedQuantification') 
        neucodeRatioBasedQuantification.appendChild(doc.createTextNode('False'))
        root.appendChild(neucodeRatioBasedQuantification)
        
        #create neucodeStabilizeLargeRatios subnode
        neucodeStabilizeLargeRatios = doc.createElement('neucodeStabilizeLargeRatios')
        neucodeStabilizeLargeRatios.appendChild(doc.createTextNode('False'))
        root.appendChild(neucodeStabilizeLargeRatios)
        
        #create rtShift subnode
        rtShift = doc.createElement('rtShift')
        rtShift.appendChild(doc.createTextNode('False'))
        root.appendChild(rtShift)
        
        # create some paramas and set default value
        separateLfq = doc.createElement('separateLfq')
        separateLfq.appendChild(doc.createTextNode('False'))
        root.appendChild(separateLfq)
        
        lfqStabilizeLargeRatios = doc.createElement('lfqStabilizeLargeRatios')
        lfqStabilizeLargeRatios.appendChild(doc.createTextNode('True'))
        root.appendChild(lfqStabilizeLargeRatios)
        
        lfqRequireMsms = doc.createElement('lfqRequireMsms')
        lfqRequireMsms.appendChild(doc.createTextNode('True'))
        root.appendChild(lfqRequireMsms)
        
        decoyMode = doc.createElement('decoyMode')
        decoyMode.appendChild(doc.createTextNode('revert'))
        root.appendChild(decoyMode)
        
        boxCarMode = doc.createElement('boxCarMode')
        boxCarMode.appendChild(doc.createTextNode('all'))
        root.appendChild(boxCarMode)
        
        includeContaminants = doc.createElement('includeContaminants')
        includeContaminants.appendChild(doc.createTextNode('True'))
        root.appendChild(includeContaminants)
        
        maxPeptideMass = doc.createElement('maxPeptideMass')
        maxPeptideMass.appendChild(doc.createTextNode('4600'))
        root.appendChild(maxPeptideMass)
        
        epsilonMutationScore = doc.createElement('epsilonMutationScore')
        epsilonMutationScore.appendChild(doc.createTextNode('True'))
        root.appendChild(epsilonMutationScore)
        
        mutatedPeptidesSeparately = doc.createElement('mutatedPeptidesSeparately')
        mutatedPeptidesSeparately.appendChild(doc.createTextNode('True'))
        root.appendChild(mutatedPeptidesSeparately)
        
        proteogenomicPeptidesSeparately = doc.createElement('proteogenomicPeptidesSeparately')
        proteogenomicPeptidesSeparately.appendChild(doc.createTextNode('True'))
        root.appendChild(proteogenomicPeptidesSeparately)
        
        minDeltaScoreUnmodifiedPeptides = doc.createElement('minDeltaScoreUnmodifiedPeptides')
        minDeltaScoreUnmodifiedPeptides.appendChild(doc.createTextNode('0'))
        root.appendChild(minDeltaScoreUnmodifiedPeptides)
        
        minDeltaScoreModifiedPeptides = doc.createElement('minDeltaScoreModifiedPeptides')
        minDeltaScoreModifiedPeptides.appendChild(doc.createTextNode('6'))
        root.appendChild(minDeltaScoreModifiedPeptides)
        
        minScoreUnmodifiedPeptides = doc.createElement('minScoreUnmodifiedPeptides')
        minScoreUnmodifiedPeptides.appendChild(doc.createTextNode('0'))
        root.appendChild(minScoreUnmodifiedPeptides)        
        
        minScoreModifiedPeptides = doc.createElement('minScoreModifiedPeptides')
        minScoreModifiedPeptides.appendChild(doc.createTextNode('40'))
        root.appendChild(minScoreModifiedPeptides)        
        
        secondPeptide = doc.createElement('secondPeptide')
        secondPeptide.appendChild(doc.createTextNode('True'))
        root.appendChild(secondPeptide)        
        
        matchBetweenRuns_node = doc.createElement('matchBetweenRuns')
        matchBetweenRuns_node.appendChild(doc.createTextNode(matchBetweenRuns))
        root.appendChild(matchBetweenRuns_node)        
        
        matchUnidentifiedFeatures = doc.createElement('matchUnidentifiedFeatures')
        matchUnidentifiedFeatures.appendChild(doc.createTextNode('False'))
        root.appendChild(matchUnidentifiedFeatures)        
        
        matchBetweenRunsFdr = doc.createElement('matchBetweenRunsFdr')
        matchBetweenRunsFdr.appendChild(doc.createTextNode('False'))
        root.appendChild(matchBetweenRunsFdr)        
        
        dependentPeptides = doc.createElement('dependentPeptides')
        dependentPeptides.appendChild(doc.createTextNode('False'))
        root.appendChild(dependentPeptides)        
        
        dependentPeptideFdr = doc.createElement('dependentPeptideFdr')
        dependentPeptideFdr.appendChild(doc.createTextNode('0'))
        root.appendChild(dependentPeptideFdr)        
        
        dependentPeptideMassBin = doc.createElement('dependentPeptideMassBin')
        dependentPeptideMassBin.appendChild(doc.createTextNode('0'))
        root.appendChild(dependentPeptideMassBin)
        
        dependentPeptidesBetweenRuns = doc.createElement('dependentPeptidesBetweenRuns')
        dependentPeptidesBetweenRuns.appendChild(doc.createTextNode('False'))
        root.appendChild(dependentPeptidesBetweenRuns)
        
        dependentPeptidesWithinExperiment = doc.createElement('dependentPeptidesWithinExperiment')
        dependentPeptidesWithinExperiment.appendChild(doc.createTextNode('False'))
        root.appendChild(dependentPeptidesWithinExperiment)
        
        dependentPeptidesWithinParameterGroup = doc.createElement('dependentPeptidesWithinParameterGroup')
        dependentPeptidesWithinParameterGroup.appendChild(doc.createTextNode('False'))
        root.appendChild(dependentPeptidesWithinParameterGroup)
        
        dependentPeptidesRestrictFractions = doc.createElement('dependentPeptidesRestrictFractions')
        dependentPeptidesRestrictFractions.appendChild(doc.createTextNode('False'))
        root.appendChild(dependentPeptidesRestrictFractions)
        
        dependentPeptidesFractionDifference = doc.createElement('dependentPeptidesFractionDifference')
        dependentPeptidesFractionDifference.appendChild(doc.createTextNode('0'))
        root.appendChild(dependentPeptidesFractionDifference)
        
        msmsConnection = doc.createElement('msmsConnection')
        msmsConnection.appendChild(doc.createTextNode('False'))
        root.appendChild(msmsConnection)

        ibaq = doc.createElement('ibaq')
        ibaq.appendChild(doc.createTextNode('False'))
        root.appendChild(ibaq)
        
        top3 = doc.createElement('top3')
        top3.appendChild(doc.createTextNode('False'))
        root.appendChild(top3)
        
        independentEnzymes = doc.createElement('independentEnzymes')
        independentEnzymes.appendChild(doc.createTextNode('False'))
        root.appendChild(independentEnzymes)
        
        useDeltaScore = doc.createElement('useDeltaScore')
        useDeltaScore.appendChild(doc.createTextNode('False'))
        root.appendChild(useDeltaScore)
        
        splitProteinGroupsByTaxonomy = doc.createElement('splitProteinGroupsByTaxonomy')
        splitProteinGroupsByTaxonomy.appendChild(doc.createTextNode('False'))
        root.appendChild(splitProteinGroupsByTaxonomy)
        
        taxonomyLevel = doc.createElement('taxonomyLevel')
        taxonomyLevel.appendChild(doc.createTextNode('Species'))
        root.appendChild(taxonomyLevel)
        
        avalon = doc.createElement('avalon')
        avalon.appendChild(doc.createTextNode('False'))
        root.appendChild(avalon)
        
        nModColumns = doc.createElement('nModColumns')
        nModColumns.appendChild(doc.createTextNode('3'))
        root.appendChild(nModColumns)
        
        ibaqLogFit = doc.createElement('ibaqLogFit')
        ibaqLogFit.appendChild(doc.createTextNode('False'))
        root.appendChild(ibaqLogFit)
        
        razorProteinFdr = doc.createElement('razorProteinFdr')
        razorProteinFdr.appendChild(doc.createTextNode('True'))
        root.appendChild(razorProteinFdr)
        
        deNovoSequencing = doc.createElement('deNovoSequencing')
        deNovoSequencing.appendChild(doc.createTextNode('False'))
        root.appendChild(deNovoSequencing)
        
        deNovoVarMods = doc.createElement('deNovoVarMods')
        deNovoVarMods.appendChild(doc.createTextNode('True'))
        root.appendChild(deNovoVarMods)
        
        massDifferenceSearch = doc.createElement('massDifferenceSearch')
        massDifferenceSearch.appendChild(doc.createTextNode('False'))
        root.appendChild(massDifferenceSearch)
        
        isotopeCalc = doc.createElement('isotopeCalc')
        isotopeCalc.appendChild(doc.createTextNode('False'))
        root.appendChild(isotopeCalc)
        
        writePeptidesForSpectrumFile = doc.createElement('writePeptidesForSpectrumFile')
        writePeptidesForSpectrumFile.appendChild(doc.createTextNode(''))
        root.appendChild(writePeptidesForSpectrumFile)
        
        intensityPredictionsFile = doc.createElement('intensityPredictionsFile')
        intensityPredictionsFile.appendChild(doc.createTextNode(''))
        root.appendChild(intensityPredictionsFile)
        
        minPepLen = doc.createElement('minPepLen')
        minPepLen.appendChild(doc.createTextNode('7'))
        root.appendChild(minPepLen)
        
        psmFdrCrosslink = doc.createElement('psmFdrCrosslink')
        psmFdrCrosslink.appendChild(doc.createTextNode('0.01'))
        root.appendChild(psmFdrCrosslink)
        
        peptideFdr = doc.createElement('peptideFdr')
        peptideFdr.appendChild(doc.createTextNode(str(peptideFDR)))
        root.appendChild(peptideFdr)
        
        proteinFdr = doc.createElement('proteinFdr')
        proteinFdr.appendChild(doc.createTextNode(str(proteinFDR)))
        root.appendChild(proteinFdr)
        
        siteFdr = doc.createElement('siteFdr')
        siteFdr.appendChild(doc.createTextNode('0.01'))
        root.appendChild(siteFdr)
        
        minPeptideLengthForUnspecificSearch = doc.createElement('minPeptideLengthForUnspecificSearch')
        minPeptideLengthForUnspecificSearch.appendChild(doc.createTextNode('8'))
        root.appendChild(minPeptideLengthForUnspecificSearch)
        
        maxPeptideLengthForUnspecificSearch = doc.createElement('maxPeptideLengthForUnspecificSearch')
        maxPeptideLengthForUnspecificSearch.appendChild(doc.createTextNode('25'))
        root.appendChild(maxPeptideLengthForUnspecificSearch)
        
        useNormRatiosForOccupancy = doc.createElement('useNormRatiosForOccupancy')
        useNormRatiosForOccupancy.appendChild(doc.createTextNode('True'))
        root.appendChild(useNormRatiosForOccupancy) 
        
        minPeptides = doc.createElement('minPeptides')
        minPeptides.appendChild(doc.createTextNode('1'))
        root.appendChild(minPeptides) 
        
        minRazorPeptides = doc.createElement('minRazorPeptides')
        minRazorPeptides.appendChild(doc.createTextNode('1'))
        root.appendChild(minRazorPeptides)
        
        minUniquePeptides = doc.createElement('minUniquePeptides')
        minUniquePeptides.appendChild(doc.createTextNode('0'))
        root.appendChild(minUniquePeptides)
        
        useCounterparts = doc.createElement('useCounterparts')
        useCounterparts.appendChild(doc.createTextNode('False'))
        root.appendChild(useCounterparts)
        
        advancedSiteIntensities = doc.createElement('advancedSiteIntensities')
        advancedSiteIntensities.appendChild(doc.createTextNode('True'))
        root.appendChild(advancedSiteIntensities) 
        
        customProteinQuantification = doc.createElement('customProteinQuantification')
        customProteinQuantification.appendChild(doc.createTextNode('False'))
        root.appendChild(customProteinQuantification)
        
        customProteinQuantificationFile = doc.createElement('customProteinQuantificationFile')
        customProteinQuantificationFile.appendChild(doc.createTextNode(''))
        root.appendChild(customProteinQuantificationFile)
        
        minRatioCount = doc.createElement('minRatioCount')
        minRatioCount.appendChild(doc.createTextNode('2'))
        root.appendChild(minRatioCount)
         
        restrictProteinQuantification = doc.createElement('restrictProteinQuantification')
        restrictProteinQuantification.appendChild(doc.createTextNode('True'))
        root.appendChild(restrictProteinQuantification)
         
        restrictMods = doc.createElement('restrictMods')
        string01 = doc.createElement('string')
        string01.appendChild(doc.createTextNode('Oxidation (M)'))
        string02 = doc.createElement('string')
        string02.appendChild(doc.createTextNode('Acetyl (Protein N-term)'))
        restrictMods.appendChild(string01)
        restrictMods.appendChild(string02)
        root.appendChild(restrictMods)
         
        matchingTimeWindow = doc.createElement('matchingTimeWindow')
        matchingIonMobilityWindow = doc.createElement('matchingIonMobilityWindow')
        alignmentTimeWindow = doc.createElement('alignmentTimeWindow')
        alignmentIonMobilityWindow = doc.createElement('alignmentIonMobilityWindow')
        if matchBetweenRuns == True:
            matchingTimeWindow.appendChild(doc.createTextNode('0.7'))
            matchingIonMobilityWindow.appendChild(doc.createTextNode('0.05'))
            alignmentTimeWindow.appendChild(doc.createTextNode('20'))
            alignmentIonMobilityWindow.appendChild(doc.createTextNode('1'))
        else:
            matchingTimeWindow.appendChild(doc.createTextNode('0'))
            matchingIonMobilityWindow.appendChild(doc.createTextNode('0'))
            alignmentTimeWindow.appendChild(doc.createTextNode('0'))
            alignmentIonMobilityWindow.appendChild(doc.createTextNode('0'))
        root.appendChild(matchingTimeWindow)
        root.appendChild(matchingIonMobilityWindow)
        root.appendChild(alignmentTimeWindow)
        root.appendChild(alignmentIonMobilityWindow)
        
#        numberOfCandidatesMultiplexedMsms = doc.createElement('numberOfCandidatesMultiplexedMsms')
#        numberOfCandidatesMultiplexedMsms.appendChild(doc.createTextNode('25'))
#        root.appendChild(numberOfCandidatesMultiplexedMsms)
        
        numberOfCandidatesMsms = doc.createElement('numberOfCandidatesMsms')
        numberOfCandidatesMsms.appendChild(doc.createTextNode('15'))
        root.appendChild(numberOfCandidatesMsms)
        
        compositionPrediction = doc.createElement('compositionPrediction')
        compositionPrediction.appendChild(doc.createTextNode('0'))
        root.appendChild(compositionPrediction)
        
        quantMode = doc.createElement('quantMode')
        quantMode.appendChild(doc.createTextNode('1'))
        root.appendChild(quantMode)
        
        massDifferenceMods = doc.createElement('massDifferenceMods')
        massDifferenceMods.appendChild(doc.createTextNode(''))
        root.appendChild(massDifferenceMods)
        
        mainSearchMaxCombinations = doc.createElement('mainSearchMaxCombinations')
        mainSearchMaxCombinations.appendChild(doc.createTextNode('200'))
        root.appendChild(mainSearchMaxCombinations)
        
        writeMsScansTable = doc.createElement('writeMsScansTable')
        writeMsScansTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writeMsScansTable)
        
        writeMsmsScansTable = doc.createElement('writeMsmsScansTable')
        writeMsmsScansTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writeMsmsScansTable)
        
        writePasefMsmsScansTable = doc.createElement('writePasefMsmsScansTable')
        writePasefMsmsScansTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writePasefMsmsScansTable)
        
        writeAccumulatedPasefMsmsScansTable = doc.createElement('writeAccumulatedPasefMsmsScansTable')
        writeAccumulatedPasefMsmsScansTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writeAccumulatedPasefMsmsScansTable)
        
        writeMs3ScansTable = doc.createElement('writeMs3ScansTable')
        writeMs3ScansTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writeMs3ScansTable)
                
        writeAllPeptidesTable = doc.createElement('writeAllPeptidesTable')
        writeAllPeptidesTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writeAllPeptidesTable)
                
        writeMzRangeTable = doc.createElement('writeMzRangeTable')
        writeMzRangeTable.appendChild(doc.createTextNode('True'))
        root.appendChild(writeMzRangeTable)
                
        writeMzTab = doc.createElement('writeMzTab')
        writeMzTab.appendChild(doc.createTextNode('True'))
        root.appendChild(writeMzTab)        
        
        disableMd5 = doc.createElement('disableMd5')
        disableMd5.appendChild(doc.createTextNode('False'))
        root.appendChild(disableMd5)        
        
        cacheBinInds = doc.createElement('cacheBinInds')
        cacheBinInds.appendChild(doc.createTextNode('True'))
        root.appendChild(cacheBinInds)        
        
        etdIncludeB = doc.createElement('etdIncludeB')
        etdIncludeB.appendChild(doc.createTextNode('False'))
        root.appendChild(etdIncludeB)
                
#        complementaryTmtCollapseNplets = doc.createElement('complementaryTmtCollapseNplets')
#        complementaryTmtCollapseNplets.appendChild(doc.createTextNode('True'))
#        root.appendChild(complementaryTmtCollapseNplets)
                
        ms2PrecursorShift = doc.createElement('ms2PrecursorShift')
        ms2PrecursorShift.appendChild(doc.createTextNode('0'))
        root.appendChild(ms2PrecursorShift)
                
        complementaryIonPpm = doc.createElement('complementaryIonPpm')
        complementaryIonPpm.appendChild(doc.createTextNode('20'))
        root.appendChild(complementaryIonPpm)
                
        variationParseRule = doc.createElement('variationParseRule')
        variationParseRule.appendChild(doc.createTextNode(''))
        root.appendChild(variationParseRule)
        
        variationMode = doc.createElement('variationMode')
        variationMode.appendChild(doc.createTextNode('none'))
        root.appendChild(variationMode)
                        
        useSeriesReporters = doc.createElement('useSeriesReporters')
        useSeriesReporters.appendChild(doc.createTextNode('False'))
        root.appendChild(useSeriesReporters)
                        
        window_name = doc.createElement('name')
        window_name.appendChild(doc.createTextNode('Session1'))
        maxquant_version = doc.createElement('maxQuantVersion')
        maxquant_version.appendChild(doc.createTextNode('1.6.10.43'))  #default version
        tempFolder_node = doc.createElement('tempFolder')
        tempFolder_node.appendChild(doc.createTextNode(tempFolder))
        pluginFolder = doc.createElement('pluginFolder')
        pluginFolder.appendChild(doc.createTextNode(''))
        numThreads_node = doc.createElement('numThreads')
        numThreads_node.appendChild(doc.createTextNode(str(numThreads)))
        emailAddress = doc.createElement('emailAddress')
        emailAddress.appendChild(doc.createTextNode(''))
        smtpHost = doc.createElement('smtpHost')
        smtpHost.appendChild(doc.createTextNode(''))
        emailFromAddress = doc.createElement('emailFromAddress')
        emailFromAddress.appendChild(doc.createTextNode(''))
        fixedCombinedFolder = doc.createElement('fixedCombinedFolder')
        fixedCombinedFolder.appendChild(doc.createTextNode(''))
        fullMinMz = doc.createElement('fullMinMz') 
        fullMaxMz = doc.createElement('fullMaxMz')
        fullMaxMz.appendChild(doc.createTextNode('1.79769313486232E+308'))
        fullMinMz.appendChild(doc.createTextNode('-1.79769313486232E+308'))  
        sendEmail = doc.createElement('sendEmail')
        sendEmail.appendChild(doc.createTextNode('False'))
        ionCountIntensities = doc.createElement('ionCountIntensities')
        ionCountIntensities.appendChild(doc.createTextNode('False'))
        verboseColumnHeaders = doc.createElement('verboseColumnHeaders')
        verboseColumnHeaders.appendChild(doc.createTextNode('False'))
        calcPeakProperties = doc.createElement('calcPeakProperties')
        calcPeakProperties.appendChild(doc.createTextNode('False'))
        showCentroidMassDifferences =doc.createElement('showCentroidMassDifferences')
        showCentroidMassDifferences.appendChild(doc.createTextNode('False'))
        showIsotopeMassDifferences = doc.createElement('showIsotopeMassDifferences')
        showIsotopeMassDifferences.appendChild(doc.createTextNode('False'))
        useDotNetCore = doc.createElement('useDotNetCore')
        useDotNetCore.appendChild(doc.createTextNode('False'))
        root.appendChild(window_name)
        root.appendChild(maxquant_version)
        root.appendChild(tempFolder_node)
        root.appendChild(pluginFolder)
        root.appendChild(numThreads_node)
        root.appendChild(emailAddress)
        root.appendChild(smtpHost)
        root.appendChild(emailFromAddress)
        root.appendChild(fixedCombinedFolder)
        root.appendChild(fullMinMz)
        root.appendChild(fullMaxMz)
        root.appendChild(sendEmail)
        root.appendChild(ionCountIntensities)
        root.appendChild(verboseColumnHeaders)
        root.appendChild(calcPeakProperties)
        root.appendChild(showCentroidMassDifferences)
        root.appendChild(showIsotopeMassDifferences)
        root.appendChild(useDotNetCore)
        
        #create raw data file path 、experients subnode,default Path = raw file name 
        # technical replicates belong to different experiments otherwise, the intensities would be combined
        filePaths = doc.createElement('filePaths')
        experiments = doc.createElement('experiments')
        for key,value in file2source.items():
            string = doc.createElement('string')
            string.appendChild(doc.createTextNode(raw_Folder + '\\' + key))
            filePaths.appendChild(string)
            string = doc.createElement('string')
            string.appendChild(doc.createTextNode(value + '_Tr_' + file2technical_rep[key]))
            experiments.appendChild(string)
        root.appendChild(filePaths)
        root.appendChild(experiments)
        
        #create fractions subnode 
        fractions = doc.createElement('fractions')
        ptms = doc.createElement('ptms')
        for key,value in file2fraction.items():
            short = doc.createElement('short')
            if value == 0:
                short_text = doc.createTextNode('32767')
            else:
                short_text = doc.createTextNode(value)
            short.appendChild(short_text)
            fractions.appendChild(short)
            
            #create PTMS subnode
            boolean = doc.createElement('boolean')
            boolean.appendChild(doc.createTextNode('False'))
            ptms.appendChild(boolean)
        root.appendChild(fractions)
        root.appendChild(ptms)
        
        #create paramGroupIndices subnode
        paramGroupIndices = doc.createElement('paramGroupIndices')
        parameterGroup = {}
        tag = 0; tmp = []
        
        referenceChannel = doc.createElement('referenceChannel')
        
        for key1,instr_val in file2instrument.items():
            value2 = str(file2enzyme[key1]) + file2label[key1] + str(file2mods[key1])
            
            if  tag == 0 and tmp == []:
                int_node = doc.createElement('int')
                int_node.appendChild(doc.createTextNode('0'))
                tmp.append({instr_val:value2})
                parameterGroup['0'] = [file2instrument[key1],file2label[key1],file2mods[key1],file2enzyme[key1]]
                
            elif {instr_val:value2} in tmp:
                int_node = doc.createElement('int')
                int_text = doc.createTextNode(str(tag))
                int_node.appendChild(int_text)
                
            else:
                tag +=1
                int_node = doc.createElement('int')
                int_text = doc.createTextNode(str(tag))
                int_node.appendChild(int_text)
                tmp.append({instr_val:value2})
                parameterGroup[str(tag)] = [file2instrument[key1],file2label[key1],file2mods[key1],file2enzyme[key1]]
    
            paramGroupIndices.appendChild(int_node)  
            
            #create referenceChannelsubnode
            
            string = doc.createElement('string')
            string.appendChild(doc.createTextNode(''))
            referenceChannel.appendChild(string)
        del tmp
        root.appendChild(paramGroupIndices)
        
        root.appendChild(referenceChannel)
        
        intensPred_node = doc.createElement('intensPred')
        intensPred_node.appendChild(doc.createTextNode('False'))
        root.appendChild(intensPred_node)
        
        intensPredModelReTrain = doc.createElement('intensPredModelReTrain')
        intensPredModelReTrain.appendChild(doc.createTextNode('False'))
        root.appendChild(intensPredModelReTrain)
        
        #create parameterGroup paramas subnode
        parameterGroups = doc.createElement('parameterGroups')
        for i,j in parameterGroup.items():
            parameterGroup = doc.createElement('parameterGroup')
            msInstrument = doc.createElement('msInstrument')
            if 'Bruker Q-TOF' == j[0]: 
                msInstrument.appendChild(doc.createTextNode('1'))
                maxCharge = doc.createElement('maxCharge')
                maxCharge.appendChild(doc.createTextNode('5'))
                minPeakLen = doc.createElement('minPeakLen')
                minPeakLen.appendChild(doc.createTextNode('3'))
                diaMinPeakLen = doc.createElement('diaMinPeakLen')
                diaMinPeakLen.appendChild(doc.createTextNode('3'))
                useMs1Centroids = doc.createElement('useMs1Centroids')
                useMs1Centroids.appendChild(doc.createTextNode('True'))
                useMs2Centroids = doc.createElement('useMs2Centroids')
                useMs2Centroids.appendChild(doc.createTextNode('True'))
                intensityDetermination = doc.createElement('intensityDetermination')
                intensityDetermination.appendChild(doc.createTextNode('0'))
                centroidMatchTol = doc.createElement('centroidMatchTol')
                centroidMatchTol.appendChild(doc.createTextNode('0.008'))
                centroidMatchTolInPpm = doc.createElement('centroidMatchTolInPpm')
                centroidMatchTolInPpm.appendChild(doc.createTextNode('True'))
                valleyFactor = doc.createElement('valleyFactor')
                valleyFactor.appendChild(doc.createTextNode('1.2'))
                advancedPeakSplitting = doc.createElement('advancedPeakSplitting')
                advancedPeakSplitting.appendChild(doc.createTextNode('True'))
                intensityThreshold = doc.createElement('intensityThreshold')
                intensityThreshold.appendChild(doc.createTextNode('30'))
            elif 'AB Sciex Q-TOF' == j[0]: 
                msInstrument.appendChild(doc.createTextNode('2'))
                maxCharge = doc.createElement('maxCharge')
                maxCharge.appendChild(doc.createTextNode('5'))
                minPeakLen = doc.createElement('minPeakLen')
                minPeakLen.appendChild(doc.createTextNode('3'))
                diaMinPeakLen = doc.createElement('diaMinPeakLen')
                diaMinPeakLen.appendChild(doc.createTextNode('3'))
                useMs1Centroids = doc.createElement('useMs1Centroids')
                useMs1Centroids.appendChild(doc.createTextNode('True'))
                useMs2Centroids = doc.createElement('useMs2Centroids')
                useMs2Centroids.appendChild(doc.createTextNode('True'))
                intensityDetermination = doc.createElement('intensityDetermination')
                intensityDetermination.appendChild(doc.createTextNode('0'))
                centroidMatchTol = doc.createElement('centroidMatchTol')
                centroidMatchTol.appendChild(doc.createTextNode('0.01'))
                centroidMatchTolInPpm = doc.createElement('centroidMatchTolInPpm')
                centroidMatchTolInPpm.appendChild(doc.createTextNode('True'))
                valleyFactor = doc.createElement('valleyFactor')
                valleyFactor.appendChild(doc.createTextNode('1.2'))
                advancedPeakSplitting = doc.createElement('advancedPeakSplitting')
                advancedPeakSplitting.appendChild(doc.createTextNode('True'))
                intensityThreshold = doc.createElement('intensityThreshold')
                intensityThreshold.appendChild(doc.createTextNode('0'))
            elif 'Agilent Q-TOF' == j[0]:
                msInstrument.appendChild(doc.createTextNode('3'))
                maxCharge = doc.createElement('maxCharge')
                maxCharge.appendChild(doc.createTextNode('5'))
                minPeakLen = doc.createElement('minPeakLen')
                minPeakLen.appendChild(doc.createTextNode('3'))
                diaMinPeakLen = doc.createElement('diaMinPeakLen')
                diaMinPeakLen.appendChild(doc.createTextNode('3'))
                useMs1Centroids = doc.createElement('useMs1Centroids')
                useMs1Centroids.appendChild(doc.createTextNode('True'))
                useMs2Centroids = doc.createElement('useMs2Centroids')
                useMs2Centroids.appendChild(doc.createTextNode('True'))
                intensityDetermination = doc.createElement('intensityDetermination')
                intensityDetermination.appendChild(doc.createTextNode('0'))
                centroidMatchTol_node = doc.createElement('centroidMatchTol')
                centroidMatchTol_node.appendChild(doc.createTextNode('0.008'))
                centroidMatchTolInPpm = doc.createElement('centroidMatchTolInPpm')
                centroidMatchTolInPpm.appendChild(doc.createTextNode('False'))
                valleyFactor = doc.createElement('valleyFactor')
                valleyFactor.appendChild(doc.createTextNode('1.2'))
                advancedPeakSplitting = doc.createElement('advancedPeakSplitting')
                advancedPeakSplitting.appendChild(doc.createTextNode('True'))
                intensityThreshold = doc.createElement('intensityThreshold')
                intensityThreshold.appendChild(doc.createTextNode('0'))
            elif 'Bruker TIMS' == j[0]: 
                msInstrument.appendChild(doc.createTextNode('4'))
                maxCharge = doc.createElement('maxCharge')
                maxCharge.appendChild(doc.createTextNode('4'))
                minPeakLen = doc.createElement('minPeakLen')
                minPeakLen.appendChild(doc.createTextNode('2'))
                diaMinPeakLen = doc.createElement('diaMinPeakLen')
                diaMinPeakLen.appendChild(doc.createTextNode('2'))
                useMs1Centroids = doc.createElement('useMs1Centroids')
                useMs1Centroids.appendChild(doc.createTextNode('True'))
                useMs2Centroids = doc.createElement('useMs2Centroids')
                useMs2Centroids.appendChild(doc.createTextNode('True'))
                intensityDetermination = doc.createElement('intensityDetermination')
                intensityDetermination.appendChild(doc.createTextNode('0'))
                centroidMatchTol = doc.createElement('centroidMatchTol')
                centroidMatchTol.appendChild(doc.createTextNode('10'))
                centroidMatchTolInPpm = doc.createElement('centroidMatchTolInPpm')
                centroidMatchTolInPpm.appendChild(doc.createTextNode('True'))
                valleyFactor = doc.createElement('valleyFactor')
                valleyFactor.appendChild(doc.createTextNode('1.2'))
                advancedPeakSplitting = doc.createElement('advancedPeakSplitting')
                advancedPeakSplitting.appendChild(doc.createTextNode('True'))
                intensityThreshold = doc.createElement('intensityThreshold')
                intensityThreshold.appendChild(doc.createTextNode('30'))
            else:
                msInstrument.appendChild(doc.createTextNode('0'))
                maxCharge = doc.createElement('maxCharge')
                maxCharge.appendChild(doc.createTextNode('7'))
                minPeakLen = doc.createElement('minPeakLen')
                minPeakLen.appendChild(doc.createTextNode('2'))
                diaMinPeakLen = doc.createElement('diaMinPeakLen')
                diaMinPeakLen.appendChild(doc.createTextNode('2'))
                useMs1Centroids = doc.createElement('useMs1Centroids')
                useMs1Centroids.appendChild(doc.createTextNode('False'))
                useMs2Centroids = doc.createElement('useMs2Centroids')
                useMs2Centroids.appendChild(doc.createTextNode('False'))
                intensityDetermination = doc.createElement('intensityDetermination')
                intensityDetermination.appendChild(doc.createTextNode('0'))
                centroidMatchTol = doc.createElement('centroidMatchTol')
                centroidMatchTol.appendChild(doc.createTextNode('8'))
                centroidMatchTolInPpm = doc.createElement('centroidMatchTolInPpm')
                centroidMatchTolInPpm.appendChild(doc.createTextNode('True'))
                valleyFactor = doc.createElement('valleyFactor')
                valleyFactor.appendChild(doc.createTextNode('1.4'))
                advancedPeakSplitting = doc.createElement('advancedPeakSplitting')
                advancedPeakSplitting.appendChild(doc.createTextNode('False'))
                intensityThreshold = doc.createElement('intensityThreshold')
                intensityThreshold.appendChild(doc.createTextNode('0'))
                

            cutPeaks = doc.createElement('cutPeaks')
            cutPeaks.appendChild(doc.createTextNode('True'))    
            gapScans = doc.createElement('gapScans')
            gapScans.appendChild(doc.createTextNode('1'))
            minTime= doc.createElement('minTime')
            minTime.appendChild(doc.createTextNode('NaN'))
            maxTime= doc.createElement('maxTime')
            maxTime.appendChild(doc.createTextNode('NaN'))
            matchType = doc.createElement('matchType')
            matchType.appendChild(doc.createTextNode('MatchFromAndTo'))
            centroidHalfWidth = doc.createElement('centroidHalfWidth')
            centroidHalfWidth.appendChild(doc.createTextNode('35'))
            centroidHalfWidthInPpm = doc.createElement('centroidHalfWidthInPpm')
            centroidHalfWidthInPpm.appendChild(doc.createTextNode('True'))
            isotopeValleyFactor = doc.createElement('isotopeValleyFactor')
            isotopeValleyFactor.appendChild(doc.createTextNode('1.2'))
            labelMods = doc.createElement('labelMods')
            string = doc.createElement('string')
            string.appendChild(doc.createTextNode(''))
            labelMods.appendChild(string)
            lcmsRunType = doc.createElement('lcmsRunType')
            lcmsRunType.appendChild(doc.createTextNode('Standard'))
            reQuantify = doc.createElement('reQuantify')
            reQuantify.appendChild(doc.createTextNode('False'))
            
            # create label subnode
            lfqMode = doc.createElement('lfqMode')
            if j[1] == 'label free sample':
                lfqMode.appendChild(doc.createTextNode('1'))
            else:
                lfqMode.appendChild(doc.createTextNode('0'))
                
            lfqSkipNorm = doc.createElement('lfqSkipNorm')
            lfqSkipNorm.appendChild(doc.createTextNode('False'))
            lfqMinEdgesPerNode = doc.createElement('lfqMinEdgesPerNode')
            lfqMinEdgesPerNode.appendChild(doc.createTextNode('3'))
            lfqAvEdgesPerNode = doc.createElement('lfqAvEdgesPerNode')
            lfqAvEdgesPerNode.appendChild(doc.createTextNode('6'))
            lfqMaxFeatures = doc.createElement('lfqMaxFeatures')
            lfqMaxFeatures.appendChild(doc.createTextNode('100000'))
            neucodeMaxPpm = doc.createElement('neucodeMaxPpm')
            neucodeMaxPpm.appendChild(doc.createTextNode('0'))
            neucodeResolution = doc.createElement('neucodeResolution')
            neucodeResolution.appendChild(doc.createTextNode('0'))
            neucodeResolutionInMda = doc.createElement('neucodeResolutionInMda')
            neucodeResolutionInMda.appendChild(doc.createTextNode('False'))
            neucodeInSilicoLowRes = doc.createElement('neucodeInSilicoLowRes')
            neucodeInSilicoLowRes.appendChild(doc.createTextNode('False'))
            fastLfq = doc.createElement('fastLfq')
            fastLfq.appendChild(doc.createTextNode('True'))
            lfqRestrictFeatures = doc.createElement('lfqRestrictFeatures')
            lfqRestrictFeatures.appendChild(doc.createTextNode('False'))
            lfqMinRatioCount = doc.createElement('lfqMinRatioCount')
            lfqMinRatioCount.appendChild(doc.createTextNode('2'))
            maxLabeledAa = doc.createElement('maxLabeledAa')
            maxLabeledAa.appendChild(doc.createTextNode('0'))
            maxNmods = doc.createElement('maxNmods')
            maxNmods.appendChild(doc.createTextNode('5'))
            maxMissedCleavages = doc.createElement('maxMissedCleavages')
            maxMissedCleavages.appendChild(doc.createTextNode('2'))
            multiplicity = doc.createElement('multiplicity')
            multiplicity.appendChild(doc.createTextNode('1'))
            enzymeMode = doc.createElement('enzymeMode')
            enzymeMode.appendChild(doc.createTextNode('0'))
            complementaryReporterType = doc.createElement('complementaryReporterType')
            complementaryReporterType.appendChild(doc.createTextNode('0'))
            reporterNormalization = doc.createElement('reporterNormalization')
            reporterNormalization.appendChild(doc.createTextNode('0'))
            neucodeIntensityMode = doc.createElement('neucodeIntensityMode')
            neucodeIntensityMode.appendChild(doc.createTextNode('0'))
            
            #create Modification subnode
            fixedModifications = doc.createElement('fixedModifications')
            variableModifications = doc.createElement('variableModifications')
            fixedM_list = [];Variable_list = []
            fixedM_list.extend(j[2][0].split(','))
            Variable_list.extend(j[2][1].split(','))
            fixedM_list = list(set(fixedM_list))
            Variable_list = list(set(Variable_list))
            for F in fixedM_list:
                string = doc.createElement('string')
                string.appendChild(doc.createTextNode(F))
                fixedModifications.appendChild(string)
            for V in Variable_list:
                string = doc.createElement('string')
                string.appendChild(doc.createTextNode(V))
                variableModifications.appendChild(string)
            
            #create enzymes subnode
            enzymes_node = doc.createElement('enzymes')
            for index in range(len(j[3])):
                string = doc.createElement('string')
                string.appendChild(doc.createTextNode(j[3][index]))
                enzymes_node.appendChild(string)
            enzymesFirstSearch = doc.createElement('enzymesFirstSearch')
            enzymesFirstSearch.appendChild(doc.createTextNode(''))
            enzymeModeFirstSearch = doc.createElement('enzymeModeFirstSearch')
            enzymeModeFirstSearch.appendChild(doc.createTextNode('0'))
            useEnzymeFirstSearch = doc.createElement('useEnzymeFirstSearch')
            useEnzymeFirstSearch.appendChild(doc.createTextNode('False'))
            
            #create variable modification
            useVariableModificationsFirstSearch = doc.createElement('useVariableModificationsFirstSearch')
            useVariableModificationsFirstSearch.appendChild(doc.createTextNode('False'))
            useMultiModification = doc.createElement('useMultiModification')
            useMultiModification.appendChild(doc.createTextNode('False'))
            multiModifications = doc.createElement('multiModifications')
            multiModifications.appendChild(doc.createTextNode(''))
            isobaricLabels = doc.createElement('isobaricLabels')
            isobaricLabels.appendChild(doc.createTextNode(''))
            neucodeLabels = doc.createElement('neucodeLabels')
            neucodeLabels.appendChild(doc.createTextNode(''))
            variableModificationsFirstSearch = doc.createElement('variableModificationsFirstSearch')
            variableModificationsFirstSearch.appendChild(doc.createTextNode(''))
            hasAdditionalVariableModifications = doc.createElement('hasAdditionalVariableModifications')
            hasAdditionalVariableModifications.appendChild(doc.createTextNode('False'))
            additionalVariableModifications = doc.createElement('additionalVariableModifications')
            additionalVariableModifications.appendChild(doc.createTextNode(''))
            additionalVariableModificationProteins = doc.createElement('additionalVariableModificationProteins')
            additionalVariableModificationProteins.appendChild(doc.createTextNode(''))
            doMassFiltering = doc.createElement('doMassFiltering')
            doMassFiltering.appendChild(doc.createTextNode('True'))
            firstSearchTol = doc.createElement('firstSearchTol')
            firstSearchTol.appendChild(doc.createTextNode('20'))
            mainSearchTol = doc.createElement('mainSearchTol')
            mainSearchTol.appendChild(doc.createTextNode('4.5'))
            searchTolInPpm = doc.createElement('searchTolInPpm')
            searchTolInPpm.appendChild(doc.createTextNode('True'))
            isotopeMatchTol = doc.createElement('isotopeMatchTol')
            isotopeMatchTol.appendChild(doc.createTextNode('2'))
            isotopeMatchTolInPpm = doc.createElement('isotopeMatchTolInPpm')
            isotopeMatchTolInPpm.appendChild(doc.createTextNode('True'))
            isotopeTimeCorrelation = doc.createElement('isotopeTimeCorrelation')
            isotopeTimeCorrelation.appendChild(doc.createTextNode('0.6'))
            theorIsotopeCorrelation = doc.createElement('theorIsotopeCorrelation')
            theorIsotopeCorrelation.appendChild(doc.createTextNode('0.6'))
            checkMassDeficit = doc.createElement('checkMassDeficit')
            checkMassDeficit.appendChild(doc.createTextNode('True'))
            recalibrationInPpm = doc.createElement('recalibrationInPpm')
            recalibrationInPpm.appendChild(doc.createTextNode('True'))
            intensityDependentCalibration = doc.createElement('intensityDependentCalibration')
            intensityDependentCalibration.appendChild(doc.createTextNode('False'))
            minScoreForCalibration = doc.createElement('minScoreForCalibration')
            minScoreForCalibration.appendChild(doc.createTextNode('70'))
            matchLibraryFile = doc.createElement('matchLibraryFile')
            matchLibraryFile.appendChild(doc.createTextNode('False'))
            libraryFile = doc.createElement('libraryFile')
            libraryFile.appendChild(doc.createTextNode(''))
            matchLibraryMassTolPpm = doc.createElement('matchLibraryMassTolPpm')
            matchLibraryMassTolPpm.appendChild(doc.createTextNode('0'))
            matchLibraryTimeTolMin = doc.createElement('matchLibraryTimeTolMin')
            matchLibraryTimeTolMin.appendChild(doc.createTextNode('0'))
            matchLabelTimeTolMin = doc.createElement('matchLabelTimeTolMin')
            matchLabelTimeTolMin.appendChild(doc.createTextNode('0'))
            reporterMassTolerance = doc.createElement('reporterMassTolerance')
            reporterMassTolerance.appendChild(doc.createTextNode('NaN'))
            reporterPif = doc.createElement('reporterPif')
            reporterPif.appendChild(doc.createTextNode('NaN'))
            filterPif = doc.createElement('filterPif')
            filterPif.appendChild(doc.createTextNode('False'))
            reporterFraction = doc.createElement('reporterFraction')
            reporterFraction.appendChild(doc.createTextNode('NaN'))
            reporterBasePeakRatio = doc.createElement('reporterBasePeakRatio')
            reporterBasePeakRatio.appendChild(doc.createTextNode('NaN'))
            timsHalfWidth = doc.createElement('timsHalfWidth')
            timsHalfWidth.appendChild(doc.createTextNode('0'))
            timsStep = doc.createElement('timsStep')
            timsStep.appendChild(doc.createTextNode('0'))
            timsResolution = doc.createElement('timsResolution')
            timsResolution.appendChild(doc.createTextNode('0'))
            timsMinMsmsIntensity = doc.createElement('timsMinMsmsIntensity')
            timsMinMsmsIntensity.appendChild(doc.createTextNode('0'))
            timsRemovePrecursor = doc.createElement('timsRemovePrecursor')
            timsRemovePrecursor.appendChild(doc.createTextNode('True'))
            timsIsobaricLabels = doc.createElement('timsIsobaricLabels')
            timsIsobaricLabels.appendChild(doc.createTextNode('False'))
            timsCollapseMsms = doc.createElement('timsCollapseMsms')
            timsCollapseMsms.appendChild(doc.createTextNode('True'))
            crosslinkSearch = doc.createElement('crosslinkSearch')
            crosslinkSearch.appendChild(doc.createTextNode('False'))
            crossLinker = doc.createElement('crossLinker')
            crossLinker.appendChild(doc.createTextNode(''))
            minMatchXl = doc.createElement('minMatchXl')
            minMatchXl.appendChild(doc.createTextNode('0'))
            minPairedPepLenXl = doc.createElement('minPairedPepLenXl')
            minPairedPepLenXl.appendChild(doc.createTextNode('6'))
            crosslinkOnlyIntraProtein = doc.createElement('crosslinkOnlyIntraProtein')
            crosslinkOnlyIntraProtein.appendChild(doc.createTextNode('False'))
            crosslinkMaxMonoUnsaturated = doc.createElement('crosslinkMaxMonoUnsaturated')
            crosslinkMaxMonoUnsaturated.appendChild(doc.createTextNode('0'))
            crosslinkMaxMonoSaturated = doc.createElement('crosslinkMaxMonoSaturated')
            crosslinkMaxMonoSaturated.appendChild(doc.createTextNode('0'))
            crosslinkMaxDiUnsaturated = doc.createElement('crosslinkMaxDiUnsaturated')
            crosslinkMaxDiUnsaturated.appendChild(doc.createTextNode('0'))
            crosslinkMaxDiSaturated = doc.createElement('crosslinkMaxDiSaturated')
            crosslinkMaxDiSaturated.appendChild(doc.createTextNode('0'))
#            crosslinkUseSeparateFasta = doc.createElement('crosslinkUseSeparateFasta')
#            crosslinkUseSeparateFasta.appendChild(doc.createTextNode('False'))
#            crosslinkCleaveModifications = doc.createElement('crosslinkCleaveModifications')
#            crosslinkCleaveModifications.appendChild(doc.createTextNode(''))
            crosslinkModifications = doc.createElement('crosslinkModifications')
            crosslinkModifications.appendChild(doc.createTextNode(''))
            crosslinkFastaFiles = doc.createElement('crosslinkFastaFiles')
            crosslinkFastaFiles.appendChild(doc.createTextNode(''))
            crosslinkSites = doc.createElement('crosslinkSites')
            crosslinkSites.appendChild(doc.createTextNode(''))
            crosslinkNetworkFiles = doc.createElement('crosslinkNetworkFiles')
            crosslinkNetworkFiles.appendChild(doc.createTextNode(''))
            
            crosslinkMode = doc.createElement('crosslinkMode')
            crosslinkMode.appendChild(doc.createTextNode('PeptidesWithCleavedLinker'))
            peakRefinement = doc.createElement('peakRefinement')
            peakRefinement.appendChild(doc.createTextNode('False'))
            isobaricSumOverWindow = doc.createElement('isobaricSumOverWindow')
            isobaricSumOverWindow.appendChild(doc.createTextNode('True'))
            isobaricWeightExponent = doc.createElement('tisobaricWeightExponent')
            isobaricWeightExponent.appendChild(doc.createTextNode('0.75'))
            diaLibraryType = doc.createElement('diaLibraryType')
            diaLibraryType.appendChild(doc.createTextNode('0'))
            diaLibraryPath = doc.createElement('diaLibraryPath')
            diaLibraryPath.appendChild(doc.createTextNode(''))
            diaPeptidePaths = doc.createElement('diaPeptidePaths')
            diaPeptidePaths.appendChild(doc.createTextNode(''))
            diaEvidencePaths = doc.createElement('diaEvidencePaths')
            diaEvidencePaths.appendChild(doc.createTextNode(''))
            diaMsmsPaths = doc.createElement('diaMsmsPaths')
            diaMsmsPaths.appendChild(doc.createTextNode(''))
            diaInitialPrecMassTolPpm = doc.createElement('diaInitialPrecMassTolPpm')
            diaInitialPrecMassTolPpm.appendChild(doc.createTextNode('20'))
            diaInitialFragMassTolPpm = doc.createElement('diaInitialFragMassTolPpm')
            diaInitialFragMassTolPpm.appendChild(doc.createTextNode('20'))
            diaCorrThresholdFeatureClustering = doc.createElement('diaCorrThresholdFeatureClustering')
            diaCorrThresholdFeatureClustering.appendChild(doc.createTextNode('0.85'))
            diaPrecTolPpmFeatureClustering = doc.createElement('diaPrecTolPpmFeatureClustering')
            diaPrecTolPpmFeatureClustering.appendChild(doc.createTextNode('2'))
            diaFragTolPpmFeatureClustering = doc.createElement('diaFragTolPpmFeatureClustering')
            diaFragTolPpmFeatureClustering.appendChild(doc.createTextNode('2'))
            diaScoreN = doc.createElement('diaScoreN')
            diaScoreN.appendChild(doc.createTextNode('7'))
            diaMinScore = doc.createElement('diaMinScore')
            diaMinScore.appendChild(doc.createTextNode('2.99'))
            diaPrecursorQuant = doc.createElement('diaPrecursorQuant')
            diaPrecursorQuant.appendChild(doc.createTextNode('False'))
            diaDiaTopNFragmentsForQuant = doc.createElement('diaDiaTopNFragmentsForQuant')
            diaDiaTopNFragmentsForQuant.appendChild(doc.createTextNode('3'))
            
            
            #appendChild in parameterGroup
            parameterGroup.appendChild(msInstrument)
            parameterGroup.appendChild(maxCharge)
            parameterGroup.appendChild(minPeakLen)
            parameterGroup.appendChild(diaMinPeakLen)
            parameterGroup.appendChild(useMs1Centroids)
            parameterGroup.appendChild(useMs2Centroids)
            parameterGroup.appendChild(cutPeaks)
            parameterGroup.appendChild(gapScans)
            parameterGroup.appendChild(minTime)
            parameterGroup.appendChild(maxTime)
            parameterGroup.appendChild(matchType)
            parameterGroup.appendChild(intensityDetermination)
            parameterGroup.appendChild(centroidMatchTol)
            parameterGroup.appendChild(centroidMatchTolInPpm)
            parameterGroup.appendChild(centroidHalfWidth)
            parameterGroup.appendChild(centroidHalfWidthInPpm)
            parameterGroup.appendChild(centroidHalfWidthInPpm)
            parameterGroup.appendChild(valleyFactor)
            parameterGroup.appendChild(isotopeValleyFactor)
            parameterGroup.appendChild(advancedPeakSplitting)
            parameterGroup.appendChild(intensityThreshold)
            parameterGroup.appendChild(labelMods)
            parameterGroup.appendChild(lcmsRunType)
            parameterGroup.appendChild(reQuantify)
            parameterGroup.appendChild(lfqMode)
            parameterGroup.appendChild(lfqSkipNorm)
            parameterGroup.appendChild(lfqMinEdgesPerNode)
            parameterGroup.appendChild(lfqAvEdgesPerNode)
            parameterGroup.appendChild(lfqMaxFeatures)
            parameterGroup.appendChild(neucodeMaxPpm)
            parameterGroup.appendChild(neucodeResolution)
            parameterGroup.appendChild(neucodeResolutionInMda)
            parameterGroup.appendChild(neucodeInSilicoLowRes)
            parameterGroup.appendChild(fastLfq)
            parameterGroup.appendChild(lfqRestrictFeatures)
            parameterGroup.appendChild(lfqMinRatioCount)
            parameterGroup.appendChild(maxLabeledAa)
            parameterGroup.appendChild(maxNmods)
            parameterGroup.appendChild(maxMissedCleavages)
            parameterGroup.appendChild(multiplicity)
            parameterGroup.appendChild(enzymeMode)
            parameterGroup.appendChild(complementaryReporterType)
            parameterGroup.appendChild(reporterNormalization)
            parameterGroup.appendChild(neucodeIntensityMode)
            parameterGroup.appendChild(fixedModifications)
            parameterGroup.appendChild(enzymes_node)
            parameterGroup.appendChild(enzymesFirstSearch)
            parameterGroup.appendChild(enzymeModeFirstSearch)
            parameterGroup.appendChild(useEnzymeFirstSearch)
            parameterGroup.appendChild(useVariableModificationsFirstSearch)
            parameterGroup.appendChild(variableModifications)
            parameterGroup.appendChild(useMultiModification)
            parameterGroup.appendChild(multiModifications)
            parameterGroup.appendChild(isobaricLabels)
            parameterGroup.appendChild(neucodeLabels)
            parameterGroup.appendChild(variableModificationsFirstSearch)
            parameterGroup.appendChild(hasAdditionalVariableModifications)
            parameterGroup.appendChild(additionalVariableModifications)
            parameterGroup.appendChild(additionalVariableModificationProteins)
            parameterGroup.appendChild(doMassFiltering)
            parameterGroup.appendChild(firstSearchTol)
            parameterGroup.appendChild(mainSearchTol)
            parameterGroup.appendChild(searchTolInPpm)
            parameterGroup.appendChild(isotopeMatchTol)
            parameterGroup.appendChild(isotopeMatchTolInPpm)
            parameterGroup.appendChild(isotopeTimeCorrelation)
            parameterGroup.appendChild(theorIsotopeCorrelation)
            parameterGroup.appendChild(checkMassDeficit)
            parameterGroup.appendChild(recalibrationInPpm)
            parameterGroup.appendChild(intensityDependentCalibration)
            parameterGroup.appendChild(minScoreForCalibration)
            parameterGroup.appendChild(matchLibraryFile)
            parameterGroup.appendChild(libraryFile)
            parameterGroup.appendChild(matchLibraryMassTolPpm)
            parameterGroup.appendChild(matchLibraryTimeTolMin)
            parameterGroup.appendChild(matchLabelTimeTolMin)
            parameterGroup.appendChild(reporterMassTolerance)
            parameterGroup.appendChild(reporterPif)
            parameterGroup.appendChild(filterPif)
            parameterGroup.appendChild(reporterFraction)
            parameterGroup.appendChild(reporterBasePeakRatio)
            parameterGroup.appendChild(timsHalfWidth)
            parameterGroup.appendChild(timsStep)
            parameterGroup.appendChild(timsResolution)
            parameterGroup.appendChild(timsMinMsmsIntensity)
            parameterGroup.appendChild(timsRemovePrecursor)
            parameterGroup.appendChild(timsIsobaricLabels)
            parameterGroup.appendChild(timsCollapseMsms)
            parameterGroup.appendChild(crosslinkSearch)
            parameterGroup.appendChild(crossLinker)
            parameterGroup.appendChild(minMatchXl)
            parameterGroup.appendChild(minPairedPepLenXl)
            parameterGroup.appendChild(crosslinkOnlyIntraProtein)
            parameterGroup.appendChild(crosslinkMaxMonoUnsaturated)
            parameterGroup.appendChild(crosslinkMaxMonoSaturated)
            parameterGroup.appendChild(crosslinkMaxDiUnsaturated)
            parameterGroup.appendChild(crosslinkMaxDiSaturated)
            #parameterGroup.appendChild(crosslinkUseSeparateFasta)
            parameterGroup.appendChild(crosslinkModifications)
            parameterGroup.appendChild(crosslinkFastaFiles)
            parameterGroup.appendChild(crosslinkSites)
            parameterGroup.appendChild(crosslinkNetworkFiles)
            parameterGroup.appendChild(crosslinkMode)
            parameterGroup.appendChild(peakRefinement)
            parameterGroup.appendChild(isobaricSumOverWindow)
            parameterGroup.appendChild(isobaricWeightExponent)
            parameterGroup.appendChild(diaLibraryType)
            parameterGroup.appendChild(diaLibraryPath)
            parameterGroup.appendChild(diaPeptidePaths)
            parameterGroup.appendChild(diaEvidencePaths)
            parameterGroup.appendChild(diaMsmsPaths)
            parameterGroup.appendChild(diaInitialPrecMassTolPpm)
            parameterGroup.appendChild(diaInitialFragMassTolPpm)
            parameterGroup.appendChild(diaCorrThresholdFeatureClustering)
            parameterGroup.appendChild(diaPrecTolPpmFeatureClustering)
            parameterGroup.appendChild(diaFragTolPpmFeatureClustering)
            parameterGroup.appendChild(diaScoreN)
            parameterGroup.appendChild(diaMinScore)
            parameterGroup.appendChild(diaPrecursorQuant)
            parameterGroup.appendChild(diaDiaTopNFragmentsForQuant)
            parameterGroups.appendChild(parameterGroup)
        root.appendChild(parameterGroups)
        
        #create msmsParamsArray subnode
        msmsParamsArray = doc.createElement('msmsParamsArray')
        for i in range(4):
            msmsParams = doc.createElement('msmsParams')
            Name = doc.createElement('Name')
            MatchTolerance= doc.createElement('MatchTolerance')
            MatchToleranceInPpm = doc.createElement('MatchToleranceInPpm')
            DeisotopeTolerance = doc.createElement('DeisotopeTolerance')
            DeisotopeToleranceInPpm = doc.createElement('DeisotopeToleranceInPpm')
            DeNovoTolerance = doc.createElement('DeNovoTolerance')
            DeNovoToleranceInPpm = doc.createElement('DeNovoToleranceInPpm')
            Deisotope = doc.createElement('Deisotope')
            Topx = doc.createElement('Topx')
            TopxInterval = doc.createElement('TopxInterval')
            HigherCharges = doc.createElement('HigherCharges')
            IncludeWater = doc.createElement('IncludeWater')
            IncludeAmmonia = doc.createElement('IncludeAmmonia')
            DependentLosses = doc.createElement('DependentLosses')
            Recalibration = doc.createElement('Recalibration')

            if i == 0:
                Name.appendChild(doc.createTextNode('FTMS'))
                MatchTolerance.appendChild(doc.createTextNode('20'))
                MatchToleranceInPpm.appendChild(doc.createTextNode('True'))
                DeisotopeTolerance.appendChild(doc.createTextNode('7'))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode('True'))
                DeNovoTolerance.appendChild(doc.createTextNode('10'))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode('True'))
                Deisotope.appendChild(doc.createTextNode('True'))
                Topx.appendChild(doc.createTextNode('12'))
                TopxInterval.appendChild(doc.createTextNode('100'))
                HigherCharges.appendChild(doc.createTextNode('True'))
                IncludeWater.appendChild(doc.createTextNode('True'))
                IncludeAmmonia.appendChild(doc.createTextNode('True'))
                DependentLosses.appendChild(doc.createTextNode('True'))
                Recalibration.appendChild(doc.createTextNode('False'))

            elif i == 1:
                Name.appendChild(doc.createTextNode('ITMS'))
                MatchTolerance.appendChild(doc.createTextNode('0.5'))
                MatchToleranceInPpm.appendChild(doc.createTextNode('False'))
                DeisotopeTolerance.appendChild(doc.createTextNode('0.15'))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode('False'))
                DeNovoTolerance.appendChild(doc.createTextNode('0.25'))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode('False'))
                Deisotope.appendChild(doc.createTextNode('False'))
                Topx.appendChild(doc.createTextNode('8'))
                TopxInterval.appendChild(doc.createTextNode('100'))
                HigherCharges.appendChild(doc.createTextNode('True'))
                IncludeWater.appendChild(doc.createTextNode('True'))
                IncludeAmmonia.appendChild(doc.createTextNode('True'))
                DependentLosses.appendChild(doc.createTextNode('True'))
                Recalibration.appendChild(doc.createTextNode('False'))

            elif i == 2:
                Name.appendChild(doc.createTextNode('TOF'))
                MatchTolerance.appendChild(doc.createTextNode('40'))
                MatchToleranceInPpm.appendChild(doc.createTextNode('True'))
                DeisotopeTolerance.appendChild(doc.createTextNode('0.01'))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode('False'))
                DeNovoTolerance.appendChild(doc.createTextNode('0.02'))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode('False'))
                Deisotope.appendChild(doc.createTextNode('True'))
                Topx.appendChild(doc.createTextNode('10'))
                TopxInterval.appendChild(doc.createTextNode('100'))
                HigherCharges.appendChild(doc.createTextNode('True'))
                IncludeWater.appendChild(doc.createTextNode('True'))
                IncludeAmmonia.appendChild(doc.createTextNode('True'))
                DependentLosses.appendChild(doc.createTextNode('True'))
                Recalibration.appendChild(doc.createTextNode('False'))

            elif i == 3:
                Name.appendChild(doc.createTextNode('Unknown'))
                MatchTolerance.appendChild(doc.createTextNode('20'))
                MatchToleranceInPpm.appendChild(doc.createTextNode('True'))
                DeisotopeTolerance.appendChild(doc.createTextNode('7'))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode('True'))
                DeNovoTolerance.appendChild(doc.createTextNode('10'))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode('True'))
                Deisotope.appendChild(doc.createTextNode('True'))
                Topx.appendChild(doc.createTextNode('12'))
                TopxInterval.appendChild(doc.createTextNode('100'))
                HigherCharges.appendChild(doc.createTextNode('True'))
                IncludeWater.appendChild(doc.createTextNode('True'))
                IncludeAmmonia.appendChild(doc.createTextNode('True'))
                DependentLosses.appendChild(doc.createTextNode('True'))
                Recalibration.appendChild(doc.createTextNode('False'))

            msmsParams.appendChild(Name)
            msmsParams.appendChild(MatchTolerance)
            msmsParams.appendChild(MatchToleranceInPpm)
            msmsParams.appendChild(DeisotopeTolerance)
            msmsParams.appendChild(DeisotopeToleranceInPpm)
            msmsParams.appendChild(DeNovoTolerance)
            msmsParams.appendChild(DeNovoToleranceInPpm)
            msmsParams.appendChild(Deisotope)
            msmsParams.appendChild(Topx)
            msmsParams.appendChild(TopxInterval)
            msmsParams.appendChild(HigherCharges)
            msmsParams.appendChild(IncludeWater)
            msmsParams.appendChild(IncludeAmmonia)
            msmsParams.appendChild(DependentLosses)
            msmsParams.appendChild(Recalibration)
            msmsParamsArray.appendChild(msmsParams)
        root.appendChild(msmsParamsArray)
        
        #create fragmentationParamsArray subnode
        fragmentationParamsArray = doc.createElement('fragmentationParamsArray')
        for i in ['CID','HCD','ETD','PQD','ETHCD','ETCID','UVPD','Unknown']:
            fragmentationParams = doc.createElement('fragmentationParams')
            fragment_Name = doc.createElement('Name')
            fragment_Name.appendChild(doc.createTextNode(i))
            Connected = doc.createElement('Connected')
            Connected.appendChild(doc.createTextNode('False'))
            ConnectedScore0 = doc.createElement('ConnectedScore0')
            ConnectedScore0.appendChild(doc.createTextNode('1'))
            ConnectedScore1 = doc.createElement('ConnectedScore1')
            ConnectedScore1.appendChild(doc.createTextNode('1'))
            ConnectedScore2 = doc.createElement('ConnectedScore2')
            ConnectedScore2.appendChild(doc.createTextNode('1'))
            InternalFragments = doc.createElement('InternalFragments')
            InternalFragments.appendChild(doc.createTextNode('False'))
            InternalFragmentWeight = doc.createElement('InternalFragmentWeight')
            InternalFragmentWeight.appendChild(doc.createTextNode('1'))
            InternalFragmentAas = doc.createElement('InternalFragmentAas')
            InternalFragmentAas.appendChild(doc.createTextNode('KRH'))
            fragmentationParams.appendChild(fragment_Name)
            fragmentationParams.appendChild(Connected)
            fragmentationParams.appendChild(ConnectedScore0)
            fragmentationParams.appendChild(ConnectedScore1)
            fragmentationParams.appendChild(ConnectedScore2)
            fragmentationParams.appendChild(InternalFragments)
            fragmentationParams.appendChild(InternalFragmentWeight)
            fragmentationParams.appendChild(InternalFragmentAas)
            fragmentationParamsArray.appendChild(fragmentationParams)
        root.appendChild(fragmentationParamsArray)
        
        
        fp = open(output_path, 'w',encoding='utf-8')
        doc.writexml(fp, indent='', addindent='\t', newl='\n', encoding="utf-8")
        fp.close()
        if len(self.warnings) != 0:
            for k, v in self.warnings.items():
                print('WARNING: "' + k + '" occured ' + str(v) + ' times.')
        print("SUCCESS Convert " + sdrf_file + " to Maxquant parameter file")
        
        #create maxquant experimental design file
    def maxquant_experiamental_design(self,sdrf_file,output):
        sdrf = pd.read_csv(sdrf_file, sep='\t')
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)
        f = open(output,'w')
        f.write('Name\tFraction\tExperiment\tPTM')
        for index, row in sdrf.iterrows():
            data_file = row['comment[data file]'][:-4]
            source_name = row['source name']   
            if 'comment[fraction identifier]' in row:
                fraction = str(row['comment[fraction identifier]'])
                if "not available" in fraction:
                    fraction = ''
            else:
                fraction = ''
            if 'comment[technical replicate]' in row:
                technical_replicate = str(row['comment[technical replicate]'])
                if "not available" in technical_replicate:
                    tr = "1"
                else:
                    tr = technical_replicate
            else:
                tr = "1"
            experiment = source_name + '_Tr_' + tr
            f.write('\n' + data_file + '\t' + fraction + '\t' + experiment + '\t')
        f.close()
        print('SUCCESS Generate maxquant experimental design file')
