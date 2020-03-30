import glob
import csv
import re
import pandas as pd

def OpenMSifyMods(sdrf_mods):
  oms_mods = list()

  for m in sdrf_mods:
    if "AC=UNIMOD" not in m:
      raise("only UNIMOD modifications supported.")

    name = re.search("NM=(.+?);", m).group(1)
		# workaround for missing PP in some sdrf TODO: fix in sdrf spec?
    if re.search("PP=(.+?);", m) == None:
      pp = "Anywhere"
    else:
      pp = re.search("PP=(.+?);", m).group(1) # one of [Anywhere, Protein N-term, Protein C-term, Any N-term, Any C-term

    if re.search("TA=(.+?);", m) == None: # TODO: missing in sdrf.
      print("Warning no TA= specified. Setting to N-term or C-term if possible" + m)
      if "C-term" in pp:
        ta = "C-term"
      elif "N-term" in pp:
        ta = "N-term"
      else:
        print("Reassignment not possible. skipping")
        pass
    else:
      ta = re.search("TA=(.+?);", m).group(1) # target amino-acid
    aa = ta.split(",") # multiply target site e.g., S,T,Y including potentially termini "C-term"

    if pp == "Protein N-term" or pp == "Protein C-term":  
      for a in aa:
        if a == "C-term" or a == "N-term": # no site specificity
          oms_mods.append(name + " (" + pp + ")") # any Protein N/C-term
        else:
          oms_mods.append(name + " (" + pp + " " + a + ")") # specific Protein N/C-term
    elif pp == "Any N-term" or pp == "Any C-term":
      pp = pp.replace("Any ", "") # in OpenMS we just use N-term and C-term
      for a in aa:			
        if a == "C-term" or aa == "N-term": # no site specificity
          oms_mods.append(name + " (" + pp + ")") # any N/C-term
        else:
	        oms_mods.append(name + " (" + pp + " " + a + ")") # specific N/C-term
    else: # Anywhere in the peptide
      for a in aa:
	      oms_mods.append(name + " (" + a + ")") # specific site in peptide
  
  return ",".join(oms_mods)

sdrf = pd.read_table("https://raw.githubusercontent.com/bigbio/proteomics-metadata-standard/master/annotated-projects/PXD018117/sdrf.tsv")

# map filename to tuple of [fixed, variable] mods
mod_cols = [c for ind, c in enumerate(sdrf) if c.startswith('comment[modification parameters]')] # columns with modification parameters
file2mods = dict()
file2pctol = dict()
file2pctolunit = dict()
file2fragtol = dict()
file2fragtolunit = dict()
file2diss = dict()
file2enzyme = dict()
file2fraction = dict()
file2label = dict()

for index, row in sdrf.iterrows():
	## extract mods
  all_mods = list(row[mod_cols])
  var_mods = [m for m in all_mods if 'MT=variable' in m]
  var_mods.sort()
  fixed_mods = [m for m in all_mods if 'MT=fixed' in m]
  fixed_mods.sort()
  raw = row['comment[data file]']
  fixed_mods_string = ""
  if fixed_mods != None:
    fixed_mods_string = OpenMSifyMods(fixed_mods)

  variable_mods_string = ""
  if var_mods != None:
    variable_mods_string = OpenMSifyMods(var_mods)

  file2mods[raw] = (fixed_mods_string, variable_mods_string)
  file2pctol[raw] = row['comment[precursor mass tolerance]']
  file2pctolunit[raw] = "ppm" # TODO 
  file2fragtol[raw] = row['comment[fragment mass tolerance]']
  file2fragtolunit[raw] = "ppm"
  file2diss[raw] = "HCD" # TODO: where to get this information from?
  file2enzyme[raw] = re.search("NE=(.+?)$", row['comment[cleavage agent details]']).group(1)
  file2fraction[raw] = row['comment[fraction identifier]']
  file2label[raw] = re.search("NM=(.+?)$", row['comment[label]']).group(1)

#output of search settings
OpenMSSearchSettingsHeader = ["Filename", "Database", "FixedModifications", "VariableModifications", "Label", "PrecursorMassTolerance", "PrecursorMassToleranceUnit", "FragmentMassTolerance", "DissociationMethod", "Enzyme"]

print("\t".join(OpenMSSearchSettingsHeader))

for index, row in sdrf.iterrows():
  raw = row["comment[data file]"]
  print(raw+"\t"+file2mods[raw][0]+"\t"+file2mods[raw][1] +"\t"+file2label[raw]+"\t"+file2pctol[raw]+"\t"+file2pctolunit[raw]+"\t"+file2fragtol[raw]+"\t"+file2fragtolunit[raw]+"\t"+file2diss[raw]+"\t"+file2enzyme[raw])


exit()

