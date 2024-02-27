"""
Created on Sun Apr 19 09:46:14 2020

@author: ChengXin
"""

import os
import re
import time
from datetime import datetime
from xml.dom.minidom import Document
from xml.dom.minidom import parse

import numpy as np
import pandas as pd

# NOTE pkg_resources is deprecated
import pkg_resources
import yaml


class Maxquant:
    def __init__(self) -> None:
        super().__init__()
        self.warnings = {}
        self.modfile = pkg_resources.resource_filename(__name__, "modifications.xml")
        self.datparamfile = pkg_resources.resource_filename(__name__, "param2sdrf.yml")

    def guess_tmt(self, lt, label_list=None):
        warning_message = "guessing TMT from number of different labels"
        self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1

        if len(label_list) == 11:
            for i in label_list:
                if i == label_list[-1]:
                    lt = lt + "TMT11plex-Lys" + i.replace("TMT", "")
                else:
                    lt = lt + "TMT10plex-Lys" + i.replace("TMT", "") + ","
        elif len(label_list) > 8:
            for i in label_list:
                if i == label_list[-1]:
                    if "N" in i or "C" in i:
                        lt = lt + "TMT10plex-Lys" + i.replace("TMT", "")
                    else:
                        lt = lt + "TMT6plex-Lys" + i.replace("TMT", "")
                else:
                    if "N" in i or "C" in i:
                        lt = lt + "TMT10plex-Lys" + i.replace("TMT", "") + ","
                    else:
                        lt = lt + "TMT6plex-Lys" + i.replace("TMT", "") + ","

        elif len(label_list) > 6:
            for i in label_list:
                if i == label_list[-1]:
                    if "N" in i or "C" in i:
                        lt = lt + "TMT8plex-Lys" + i.replace("TMT", "")
                    else:
                        lt = lt + "TMT6plex-Lys" + i.replace("TMT", "")
                else:
                    if "N" in i or "C" in i:
                        lt = lt + "TMT8plex-Lys" + i.replace("TMT", "") + ","
                    else:
                        lt = lt + "TMT6plex-Lys" + i.replace("TMT", "") + ","
        elif len(label_list) > 2:
            for i in label_list:
                if i == label_list[-1]:
                    lt = lt + "TMT6plex-Lys" + i.replace("TMT", "").rstrip()
                else:
                    lt = lt + "TMT6plex-Lys" + i.replace("TMT", "").rstrip() + ","
        else:
            for i in label_list:
                if i == label_list[-1]:
                    lt = lt + "TMT2plex-Lys" + i.replace("TMT", "")
                else:
                    lt = lt + "TMT2plex-Lys" + i.replace("TMT", "") + ","
        return lt

    def extract_tmt_info(self, label="TMT2", mods=None):
        lt = ""
        label_list = sorted(label)
        label_head = [re.search(r"TMT(\d+)plex-", i).group(1) for i in mods if "TMT" in i]

        if len(label_head) > 0 and int(label_head[0]) >= len(label_list):
            label_head = "TMT" + label_head[0] + "plex"
            for i in label_list:
                if i == label_list[-1]:
                    lt = lt + label_head + "-Lys" + i.replace("TMT", "")
                else:
                    lt = lt + label_head + "-Lys" + i.replace("TMT", "") + ","
        else:
            lt = self.guess_tmt(lt, label_list)
        return lt

    def create_new_mods(self, mods, mqconfdir):
        i = 0
        w = y = False
        all_mods = []
        mq_name = []
        mq_position = []
        mq_site = {}
        mq_title = []
        while i < mods.shape[1]:
            all_mods.extend([j for j in list(set(mods.iloc[:, i])) if "NT" in j])
            i += 1
        all_mods = list(set(all_mods))
        # create a modification.local.xml to add new modifications
        mod_local = Document()
        root = mod_local.createElement("modifications")
        root.setAttribute("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        mod_local.appendChild(root)

        mod_file = mqconfdir + "modifications.xml"
        mod_local_file = mqconfdir + "modifications.local.xml"
        domTree = parse(mod_file)
        rootNode = domTree.documentElement
        modifications = rootNode.getElementsByTagName("modification")
        mod_pattern = re.compile(r"(.*?) \(")

        for modification in modifications:
            title = str(modification.getAttribute("title"))
            mq_title.append(title)

            mq_position.append(modification.getElementsByTagName("position")[0].childNodes[0].data)

            nodes_site = modification.getElementsByTagName("modification_site")
            temp = []
            for node in nodes_site:
                temp.append(node.getAttribute("site"))
            mq_site[title] = temp

            if "(" in title:
                title = re.search(mod_pattern, title).group(1)
            mq_name.append(title.lower())

        for mod in all_mods:
            aa_equal = False
            if "AC=UNIMOD" not in mod and "AC=Unimod" not in mod:
                warning_message = "only UNIMOD modifications supported. skip" + mod
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                continue

            name = re.search("NT=(.+?)(;|$)", mod).group(1)
            if "Label:" in name:
                warning_message = name + " Label modifications is Automatically supplemented by MaxQuant"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                continue

            if re.search("PP=(.+?)(;|$)", mod) is None:
                pp = "anywhere"
            else:
                pp = re.search("PP=(.+?)(;|$)", mod).group(
                    1
                )  # one of [Anywhere, Protein N-term, Protein C-term, Any N-term, Any C-term
            pp = pp.replace(" ", "").replace("-", "").lower()
            if re.search("TA=(.+?)(;|$)", mod) is None:
                warning_message = "Warning no TA= specified."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                aa = ["-"]
            else:
                ta = re.search("TA=(.+?)(;|$)", mod).group(1)  # target amino-acid
                if ta.lower() == "c-term":
                    pp = "anycterm"
                    aa = ["-"]
                elif ta.lower() == "n-term":
                    pp = "anynterm"
                    aa = ["-"]
                else:
                    aa = ta.split(",")  # multiply target site e.g., S,T,Y
            if re.search("CF=(.+?)(;|$)", mod) is None:
                warning_message = "Warning no CF= specified.Please add manually"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                CF = ""
            else:
                CF = re.search("CF=(.+?)(;|$)", mod).group(1).replace(" ", "").replace(")", ") ").rstrip()

            if name.lower().startswith("tmt"):
                w = True
                warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement manuanly some parameters"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                modification = mod_local.createElement("modification")
                if "-" in aa:
                    if pp == "anycterm":
                        name = name + "-" + "Cter"
                    elif pp == "anynterm":
                        name = name + "-" + "Nter"
                else:
                    name = name + "-"
                    for s in aa:
                        if s == "C":
                            name = name + "Cys"
                        elif s == "K":
                            name = name + "Lys"

                modification.setAttribute("title", name)
                modification.setAttribute("description", name + " modification")
                now_stamp = time.time()
                local_time = datetime.fromtimestamp(now_stamp)
                utc_time = datetime.utcfromtimestamp(now_stamp)
                if local_time > utc_time:
                    offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                else:
                    offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                modification.setAttribute(
                    "create_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                )
                modification.setAttribute(
                    "last_modified_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                )
                modification.setAttribute("user", "root")
                modification.setAttribute("reporterCorrectionM2", "0")
                modification.setAttribute("reporterCorrectionM1", "0")
                modification.setAttribute("reporterCorrectionP1", "0")
                modification.setAttribute("reporterCorrectionP2", "0")
                modification.setAttribute("reporterCorrectionType", "false")
                modification.setAttribute("composition", CF)
                pos = mod_local.createElement("position")
                if "nterm" in pp or "cterm" in pp:
                    pp = pp.replace("cterm", "Cterm").replace("nterm", "Nterm")
                pos.appendChild(mod_local.createTextNode(pp))
                modification.appendChild(pos)
                for i in aa:
                    mod_site = mod_local.createElement("modification_site")
                    mod_site.setAttribute("site", i)
                    neu_col = mod_local.createElement("neutralloss_collection")
                    dia_col = mod_local.createElement("diagnostic_collection")
                    mod_site.appendChild(neu_col)
                    mod_site.appendChild(dia_col)
                    modification.appendChild(mod_site)
                mod_type = mod_local.createElement("type")
                if "->" in name:
                    mod_type.appendChild(mod_local.createTextNode("AaSubstitution"))
                else:
                    mod_type.appendChild(mod_local.createTextNode("Standard"))
                modification.appendChild(mod_type)
                ter_type = mod_local.createElement("terminus_type")
                ter_type.appendChild(mod_local.createTextNode("none"))
                modification.appendChild(ter_type)
                root.appendChild(modification)
                continue

            if name.lower().startswith("itraq"):
                w = True
                name = name.strip()
                warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement manuanly some parameters"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                modification = mod_local.createElement("modification")
                if "-" in aa:
                    if pp == "anycterm":
                        name = name + "-" + "Cter"
                    elif pp == "anynterm":
                        name = name + "-" + "Nter"
                else:
                    name = name + "-"
                    for s in aa:
                        if s == "K":
                            name = name + "Lys"
                        else:
                            name = name + s

                modification.setAttribute("title", name)
                modification.setAttribute("description", name + " modification")
                now_stamp = time.time()
                local_time = datetime.fromtimestamp(now_stamp)
                utc_time = datetime.utcfromtimestamp(now_stamp)
                if local_time > utc_time:
                    offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                else:
                    offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                modification.setAttribute(
                    "create_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                )
                modification.setAttribute(
                    "last_modified_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                )
                modification.setAttribute("user", "root")
                modification.setAttribute("reporterCorrectionM2", "0")
                modification.setAttribute("reporterCorrectionM1", "0")
                modification.setAttribute("reporterCorrectionP1", "0")
                modification.setAttribute("reporterCorrectionP2", "0")
                modification.setAttribute("reporterCorrectionType", "false")
                modification.setAttribute("composition", CF)
                pos = mod_local.createElement("position")
                if "nterm" in pp or "cterm" in pp:
                    pp = pp.replace("cterm", "Cterm").replace("nterm", "Nterm")
                pos.appendChild(mod_local.createTextNode(pp))
                modification.appendChild(pos)
                for i in aa:
                    mod_site = mod_local.createElement("modification_site")
                    mod_site.setAttribute("site", i)
                    neu_col = mod_local.createElement("neutralloss_collection")
                    dia_col = mod_local.createElement("diagnostic_collection")
                    mod_site.appendChild(neu_col)
                    mod_site.appendChild(dia_col)
                    modification.appendChild(mod_site)
                mod_type = mod_local.createElement("type")
                if "->" in name:
                    mod_type.appendChild(mod_local.createTextNode("AaSubstitution"))
                else:
                    mod_type.appendChild(mod_local.createTextNode("Standard"))
                modification.appendChild(mod_type)
                ter_type = mod_local.createElement("terminus_type")
                ter_type.appendChild(mod_local.createTextNode("none"))
                modification.appendChild(ter_type)
                root.appendChild(modification)
                continue

            if "->" in name or "13C6-15N4" == name:
                pass
            else:
                name = name.capitalize()

            if "Deamidated" == name:
                name = "Deamidation"

            if name.lower() in mq_name:
                ta_tag = 0
                pp_tag = 0
                indexes = [i for i, x in enumerate(mq_name) if x == name.lower()]

                for index in indexes:
                    if pp == mq_position[index].lower():
                        pp_tag = 1
                        pp_index = index
                        a = [x for x in aa if x in mq_site[mq_title[index]]]
                        if not [y for y in (aa + mq_site[mq_title[index]]) if y not in a]:
                            ta_tag = 1
                            break
                    if aa == mq_site[mq_title[index]]:
                        ta_index = index
                        aa_equal = True
                if pp_tag == 0:
                    if "->" in name:
                        warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement"
                        self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                        y = True
                        if "nterm" in pp or "cterm" in pp:
                            pp = pp.replace("cterm", "Cterm").replace("nterm", "Nterm")
                        modifications[indexes[0]].getElementsByTagName("position")[0].childNodes[0].data = pp
                        now_stamp = time.time()
                        local_time = datetime.fromtimestamp(now_stamp)
                        utc_time = datetime.utcfromtimestamp(now_stamp)
                        if local_time > utc_time:
                            offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                        else:
                            offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                        modifications[indexes[0]].setAttribute(
                            "last_modified_date",
                            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset,
                        )
                        modifications[indexes[0]].setAttribute("user", "root")

                    elif aa_equal:
                        warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement"
                        self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                        y = True
                        if "nterm" in pp or "cterm" in pp:
                            pp = pp.replace("cterm", "Cterm").replace("nterm", "Nterm")
                        modifications[ta_index].getElementsByTagName("position")[0].childNodes[0].data = pp
                        now_stamp = time.time()
                        local_time = datetime.fromtimestamp(now_stamp)
                        utc_time = datetime.utcfromtimestamp(now_stamp)
                        if local_time > utc_time:
                            offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                        else:
                            offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                        modifications[ta_index].setAttribute(
                            "last_modified_date",
                            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset,
                        )
                        modifications[ta_index].setAttribute("user", "root")

                    else:
                        warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement"
                        w = True
                        self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                        modification = mod_local.createElement("modification")
                        if "-" in aa:
                            pass
                        else:
                            name = name + " ("
                            for s in aa:
                                name = name + s
                            name = name + ")"
                        modification.setAttribute("title", name)
                        modification.setAttribute("description", name + " modification")
                        now_stamp = time.time()
                        local_time = datetime.fromtimestamp(now_stamp)
                        utc_time = datetime.utcfromtimestamp(now_stamp)
                        if local_time > utc_time:
                            offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                        else:
                            offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                        modification.setAttribute(
                            "create_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                        )
                        modification.setAttribute(
                            "last_modified_date",
                            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset,
                        )
                        modification.setAttribute("user", "root")
                        modification.setAttribute("reporterCorrectionM2", "0")
                        modification.setAttribute("reporterCorrectionM1", "0")
                        modification.setAttribute("reporterCorrectionP1", "0")
                        modification.setAttribute("reporterCorrectionP2", "0")
                        modification.setAttribute("reporterCorrectionType", "false")
                        modification.setAttribute("composition", CF)
                        pos = mod_local.createElement("position")
                        if "nterm" in pp or "cterm" in pp:
                            pp = pp.replace("cterm", "Cterm").replace("nterm", "Nterm")
                        pos.appendChild(mod_local.createTextNode(pp))
                        modification.appendChild(pos)
                        for i in aa:
                            mod_site = mod_local.createElement("modification_site")
                            mod_site.setAttribute("site", i)
                            neu_col = mod_local.createElement("neutralloss_collection")
                            dia_col = mod_local.createElement("diagnostic_collection")
                            mod_site.appendChild(neu_col)
                            mod_site.appendChild(dia_col)
                            modification.appendChild(mod_site)
                        mod_type = mod_local.createElement("type")
                        if "->" in name:
                            mod_type.appendChild(mod_local.createTextNode("AaSubstitution"))
                        else:
                            mod_type.appendChild(mod_local.createTextNode("Standard"))
                        modification.appendChild(mod_type)
                        ter_type = mod_local.createElement("terminus_type")
                        ter_type.appendChild(mod_local.createTextNode("none"))
                        modification.appendChild(ter_type)
                        root.appendChild(modification)

                if ta_tag == 0 and pp_tag == 1:
                    w = True
                    warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement"
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                    modification = mod_local.createElement("modification")
                    if "-" in aa or "->" in name:
                        pass
                    else:
                        name = name + " ("
                        for s in aa:
                            name = name + s
                        name = name + ")"
                    modification.setAttribute("title", name)
                    modification.setAttribute("description", name + " modification")
                    now_stamp = time.time()
                    local_time = datetime.fromtimestamp(now_stamp)
                    utc_time = datetime.utcfromtimestamp(now_stamp)
                    if local_time > utc_time:
                        offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                    else:
                        offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                    modification.setAttribute(
                        "create_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                    )
                    modification.setAttribute(
                        "last_modified_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                    )
                    modification.setAttribute("user", "root")
                    modification.setAttribute("reporterCorrectionM2", "0")
                    modification.setAttribute("reporterCorrectionM1", "0")
                    modification.setAttribute("reporterCorrectionP1", "0")
                    modification.setAttribute("reporterCorrectionP2", "0")
                    modification.setAttribute("reporterCorrectionType", "false")
                    modification.setAttribute("composition", CF)
                    pos = mod_local.createElement("position")
                    pos.appendChild(mod_local.createTextNode(mq_position[pp_index]))
                    modification.appendChild(pos)
                    for i in aa:
                        mod_site = mod_local.createElement("modification_site")
                        mod_site.setAttribute("site", i)
                        neu_col = mod_local.createElement("neutralloss_collection")
                        dia_col = mod_local.createElement("diagnostic_collection")
                        mod_site.appendChild(neu_col)
                        mod_site.appendChild(dia_col)
                        modification.appendChild(mod_site)
                    mod_type = mod_local.createElement("type")
                    if "->" in name:
                        mod_type.appendChild(mod_local.createTextNode("AaSubstitution"))
                    else:
                        mod_type.appendChild(mod_local.createTextNode("Standard"))
                    modification.appendChild(mod_type)
                    ter_type = mod_local.createElement("terminus_type")
                    ter_type.appendChild(mod_local.createTextNode("none"))
                    modification.appendChild(ter_type)
                    root.appendChild(modification)
            else:
                w = True
                warning_message = "Warning no " + mod + " modification in MaxQuant.Supplement"
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                modification = mod_local.createElement("modification")
                if "-" in aa or "->" in name:
                    pass
                else:
                    name = name + " ("
                    for s in aa:
                        name = name + s
                    name = name + ")"
                modification.setAttribute("title", name)
                modification.setAttribute("description", name + " modification")
                now_stamp = time.time()
                local_time = datetime.fromtimestamp(now_stamp)
                utc_time = datetime.utcfromtimestamp(now_stamp)
                if local_time > utc_time:
                    offset = "+" + str(local_time - utc_time).zfill(8)[:5]
                else:
                    offset = "-" + str(utc_time - local_time).zfill(8)[:5]
                modification.setAttribute(
                    "create_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                )
                modification.setAttribute(
                    "last_modified_date", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+08.00")[:-7] + offset
                )
                modification.setAttribute("user", "root")
                modification.setAttribute("reporterCorrectionM2", "0")
                modification.setAttribute("reporterCorrectionM1", "0")
                modification.setAttribute("reporterCorrectionP1", "0")
                modification.setAttribute("reporterCorrectionP2", "0")
                modification.setAttribute("reporterCorrectionType", "false")
                modification.setAttribute("composition", CF)
                pos = mod_local.createElement("position")
                if "nterm" in pp or "cterm" in pp:
                    pp = pp.replace("cterm", "Cterm").replace("nterm", "Nterm")
                pos.appendChild(mod_local.createTextNode(pp))
                modification.appendChild(pos)
                for i in aa:
                    mod_site = mod_local.createElement("modification_site")
                    mod_site.setAttribute("site", i)
                    neu_col = mod_local.createElement("neutralloss_collection")
                    dia_col = mod_local.createElement("diagnostic_collection")
                    mod_site.appendChild(neu_col)
                    mod_site.appendChild(dia_col)
                    modification.appendChild(mod_site)
                mod_type = mod_local.createElement("type")
                if "->" in name:
                    mod_type.appendChild(mod_local.createTextNode("AaSubstitution"))
                else:
                    mod_type.appendChild(mod_local.createTextNode("Standard"))
                modification.appendChild(mod_type)
                ter_type = mod_local.createElement("terminus_type")
                ter_type.appendChild(mod_local.createTextNode("none"))
                modification.appendChild(ter_type)
                root.appendChild(modification)
        if w:
            with open(mod_local_file, "w", encoding="utf-8") as fp:
                mod_local.writexml(fp, indent="", addindent="\t", newl="\n", encoding="utf-8")
        if y:
            with open(mod_file, "w", encoding="utf-8") as fp:
                domTree.writexml(fp, encoding="utf-8")

    def maxquant_ify_mods(self, sdrf_mods, mqconfdir):
        mq_mods_file = self.modfile
        mod_pattern = re.compile(r"(.*?) \(")
        if mqconfdir:
            mq_new_mods = mqconfdir + "modifications.local.xml"
            dT = parse(mq_new_mods)
            rootN = dT.documentElement
            new_mods = rootN.getElementsByTagName("modification")
            new_title = []
            new_position = []
            new_name = []
            new_site = {}
            for modification in new_mods:
                title = str(modification.getAttribute("title"))
                new_title.append(title)
                new_position.append(modification.getElementsByTagName("position")[0].childNodes[0].data)
                nodes_site = modification.getElementsByTagName("modification_site")
                temp = []
                for node in nodes_site:
                    temp.append(node.getAttribute("site"))
                new_site[title] = temp
                if "iTRAQ" in title or "TMT" in title:
                    pass
                elif "(" in title:
                    title = re.search(mod_pattern, title).group(1)
                new_name.append(title.lower())

        domTree = parse(mq_mods_file)
        rootNode = domTree.documentElement
        modifications = rootNode.getElementsByTagName("modification")
        oms_mods = []
        mq_title = []
        mq_position = []
        mq_name = []
        mq_site = {}
        for modification in modifications:
            title = str(modification.getAttribute("title"))
            mq_title.append(title)
            mq_position.append(modification.getElementsByTagName("position")[0].childNodes[0].data)
            nodes_site = modification.getElementsByTagName("modification_site")
            temp = []
            for node in nodes_site:
                temp.append(node.getAttribute("site"))
            mq_site[title] = temp
            if "(" in title:
                title = re.search(mod_pattern, title).group(1)
            mq_name.append(title.lower())
        for m in sdrf_mods:
            if "AC=UNIMOD" not in m and "AC=Unimod" not in m:
                warning_message = "only UNIMOD modifications supported. skip " + m
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                continue
            name = re.search("NT=(.+?)(;|$)", m).group(1)

            # workaround for missing PP in some sdrf
            if re.search("PP=(.+?)(;|$)", m) is None:
                pp = "anywhere"
            else:
                pp = re.search("PP=(.+?)(;|$)", m).group(
                    1
                )  # one of [Anywhere, Protein N-term,Protein C-term,Any N-term,Any C-term,not C-term,not N-term
            pp = pp.replace(" ", "").replace("-", "").lower()

            if re.search("TA=(.+?)(;|$)", m) is None:
                aa = ["-"]
            else:
                ta = re.search("TA=(.+?)(;|$)", m).group(1)  # target amino-acid
                if ta.lower() == "c-term":
                    pp = "anycterm"
                    aa = ["-"]
                elif ta.lower() == "n-term":
                    pp = "anynterm"
                    aa = ["-"]
                else:
                    aa = ta.split(",")  # multiply target site e.g., S,T,Y
            if name.startswith("Label"):
                if aa[0] == "K" and name == "Label:13C(6)15N(2)":
                    oms_mods.append("Lys8")
                elif aa[0] == "K" and name == "Label:13C(6)":
                    oms_mods.append("Lys6")
                elif aa[0] == "K" and name == "Label:2H(4)":
                    oms_mods.append("Lys4")
                elif aa[0] == "R" and name == "Label:13C(6)15N(4)":
                    oms_mods.append("Arg10")
                elif aa[0] == "R" and name == "Label:13C(6)":
                    oms_mods.append("Arg6")
                else:
                    warning_message = "modification is not supported in MaxQuant. skip " + m
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                continue

            if name.lower().startswith("tmt"):
                if "-" in aa:
                    if pp == "anycterm":
                        name = name + "-" + "Cter"
                    elif pp == "anynterm":
                        name = name + "-" + "Nter"
                else:
                    name = name + "-"
                    for s in aa:
                        if s == "C":
                            name = name + "Cys"
                        elif s == "K":
                            name = name + "Lys"
                oms_mods.append(name)
                continue

            if name.lower().startswith("itraq"):
                name = name.strip()
                if "-" in aa:
                    if pp == "anycterm":
                        name = name + "-" + "Cter"
                    elif pp == "anynterm":
                        name = name + "-" + "Nter"
                else:
                    name = name + "-"
                    for s in aa:
                        if s == "K":
                            name = name + "Lys"
                        else:
                            name = name + s
                oms_mods.append(name)
                continue

            if "->" not in name:
                name = name.capitalize()

            if "Deamidated" == name:
                name = "Deamidation"

            if name.lower() in mq_name:
                tag = 0
                indexes = [i for i, x in enumerate(mq_name) if x == name.lower()]
                for index in indexes:
                    if mq_position[index].lower() == pp and aa == mq_site[mq_title[index]]:
                        tag = 1
                        break
                if tag == 1:
                    oms_mods.append(mq_title[index])

                elif mqconfdir:
                    if name.lower() in new_name and new_position[new_name.index(name.lower())].lower() == pp:
                        if aa == new_site[new_title[new_name.index(name.lower())]]:
                            index = new_name.index(name.lower())
                            oms_mods.append(new_title[index])
                else:
                    warning_message = "modification is not supported in MaxQuant. skip " + m
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1

            elif mqconfdir:
                if name.lower() in new_name and new_position[new_name.index(name.lower())].lower() == pp:
                    if aa == new_site[new_title[new_name.index(name.lower())]]:
                        index = new_name.index(name.lower())
                        oms_mods.append(new_title[index])
            else:
                warning_message = "modification is not supported in MaxQuant. skip " + m
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1

        return ",".join(oms_mods)

    def maxquant_convert(
        self,
        sdrf_file,
        fastaFilePath,
        mqconfdir,
        matchBetweenRuns,
        peptideFDR,
        proteinFDR,
        tempFolder,
        raw_Folder,
        numThreads,
        output_path,
    ):
        print("PROCESSING: " + sdrf_file + '"')

        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)  # convert column names to lower-case

        with open(self.datparamfile) as file:
            param_mapping = yaml.safe_load(file)
            mapping = param_mapping["parameters"]
        datparams = {}
        for i in mapping:
            datparams[i["sdrf"]] = i["name"]

        # map filename to tuple of [fixed, variable] mods
        mod_cols = [
            c for ind, c in enumerate(sdrf) if c.startswith("comment[modification parameters")
        ]  # columns with modification parameters

        label_cols = [c for ind, c in enumerate(sdrf) if c.startswith("comment[label")]  # columns with label parameters

        enzy_cols = [c for ind, c in enumerate(sdrf) if c.startswith("comment[cleavage agent details]")]

        file2mods = {}
        file2pctol = {}
        file2pctolunit = {}
        file2fragtol = {}
        file2fragtolunit = {}
        file2diss = {}
        file2enzyme = {}
        file2fraction = {}
        file2label = {}
        file2silac_shape = {}
        file2source = {}
        source_name_list = []
        source_name2n_reps = {}
        file2technical_rep = {}
        file2instrument = {}
        # New parameters from extended SDRF (including data analysis parameters)
        file2params = {}
        for p in datparams.values():
            file2params[p] = {}

        if mqconfdir:
            self.create_new_mods(sdrf[mod_cols], mqconfdir)

        for index, row in sdrf.iterrows():
            all_enzy = list(row[enzy_cols])

            # extract mods
            all_mods = list(row[mod_cols])

            # print(all_mods)
            var_mods = [
                m for m in all_mods if "MT=variable" in m or "MT=Variable" in m
            ]  # workaround for capitalization
            var_mods.sort()
            fixed_mods = [m for m in all_mods if "MT=fixed" in m or "MT=Fixed" in m]  # workaround for capitalization
            fixed_mods.sort()

            raw = row["comment[data file]"]

            fixed_mods_string = ""
            if fixed_mods is not None:
                fixed_mods_string = self.maxquant_ify_mods(fixed_mods, mqconfdir)

            variable_mods_string = ""
            if var_mods is not None:
                variable_mods_string = self.maxquant_ify_mods(var_mods, mqconfdir)

            file2mods[raw] = (fixed_mods_string, variable_mods_string)
            source_name = row["source name"]

            file2source[raw] = source_name

            if source_name not in source_name_list:
                source_name_list.append(source_name)

            file2instrument[raw] = row["comment[instrument]"]

            if "comment[precursor mass tolerance]" in row:
                pc_tol_str = row["comment[precursor mass tolerance]"]
                if "ppm" in pc_tol_str or "Da" in pc_tol_str:
                    pc_tmp = pc_tol_str.split(" ")
                    file2pctol[raw] = pc_tmp[0]
                    file2pctolunit[raw] = pc_tmp[1]
                else:
                    warning_message = "Invalid precursor mass tolerance set. Assuming 4.5 ppm."
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                    file2pctol[raw] = "4.5"
                    file2pctolunit[raw] = "ppm"
            else:
                warning_message = "No precursor mass tolerance set. Assuming 4.5 ppm."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                file2pctol[raw] = "4.5"
                file2pctolunit[raw] = "ppm"

            if "comment[fragment mass tolerance]" in row:
                f_tol_str = row["comment[fragment mass tolerance]"]
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

            # STILL NOT IMPLEMENTED!
            if "comment[dissociation method]" in row:
                if row["comment[dissociation method]"] == "not available":
                    file2diss[raw] = "HCD"
                else:
                    diss_method = re.search("NT=(.+?)(;|$)", row["comment[dissociation method]"]).group(1)
                    file2diss[raw] = diss_method.upper()
            else:
                warning_message = "No dissociation method provided. Assuming HCD."
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                file2diss[raw] = "HCD"

            if "comment[technical replicate]" in row:
                technical_replicate = str(row["comment[technical replicate]"])
                if "not available" in technical_replicate or "not applicable" in technical_replicate:
                    file2technical_rep[raw] = "1"
                else:
                    file2technical_rep[raw] = technical_replicate
            else:
                file2technical_rep[raw] = "1"

            # store highest replicate number for this source name
            if source_name in source_name2n_reps:
                source_name2n_reps[source_name] = max(
                    int(source_name2n_reps[source_name]), int(file2technical_rep[raw])
                )
            else:
                source_name2n_reps[source_name] = int(file2technical_rep[raw])

            e_list = []
            for e in all_enzy:
                enzyme = re.search("NT=(.+?)(;|$)", e).group(1)
                enzyme = enzyme.capitalize()
                if "Trypsin/p" in enzyme:  # workaround
                    enzyme = "Trypsin/P"
                elif "Lys-c" in enzyme:
                    enzyme = "LysC"
                e_list.append(enzyme)
            file2enzyme[raw] = e_list
            # print(enzyme)

            if "comment[fraction identifier]" in row:
                fraction = str(row["comment[fraction identifier]"])
                if "not available" in fraction or not fraction.isdigit():
                    file2fraction[raw] = 0
                else:
                    file2fraction[raw] = fraction
            else:
                file2fraction[raw] = 0

            # For different quantitative experiments
            if "not available" in row["comment[label]"] or "not applicable" in row["comment[label]"]:
                file2label[raw] = "label free sample"
            elif re.search("NT=(.+?)(;|$)", row["comment[label]"]) is not None:
                label = re.search("NT=(.+?)(;|$)", row["comment[label]"]).group(1)
                file2label[raw] = label
            elif row["comment[label]"].lower() == "ibaq":
                file2label[raw] = "iBAQ"
            elif row["comment[label]"].startswith("TMT"):
                lt = ""
                label_list = sorted(list(sdrf[sdrf["comment[data file]"] == raw]["comment[label]"]))
                label_head = [re.search(r"TMT(\d+)plex-", i).group(1) for i in file2mods[raw] if "TMT" in i]

                if len(label_head) > 0 and int(label_head[0]) >= len(label_list):
                    label_head = "TMT" + label_head[0] + "plex"
                    for i in label_list:
                        if i == label_list[-1]:
                            lt = lt + label_head + "-Lys" + i.replace("TMT", "")
                        else:
                            lt = lt + label_head + "-Lys" + i.replace("TMT", "") + ","
                else:
                    if len(label_list) == 11:
                        for i in label_list:
                            if i == label_list[-1]:
                                lt = lt + "TMT11plex-Lys" + i.replace("TMT", "")
                            else:
                                lt = lt + "TMT10plex-Lys" + i.replace("TMT", "") + ","
                    elif len(label_list) > 8:
                        for i in label_list:
                            if i == label_list[-1]:
                                if "N" in i or "C" in i:
                                    lt = lt + "TMT10plex-Lys" + i.replace("TMT", "")
                                else:
                                    lt = lt + "TMT6plex-Lys" + i.replace("TMT", "")
                            else:
                                if "N" in i or "C" in i:
                                    lt = lt + "TMT10plex-Lys" + i.replace("TMT", "") + ","
                                else:
                                    lt = lt + "TMT6plex-Lys" + i.replace("TMT", "") + ","

                    elif len(label_list) > 6:
                        for i in label_list:
                            if i == label_list[-1]:
                                if "N" in i or "C" in i:
                                    lt = lt + "TMT8plex-Lys" + i.replace("TMT", "")
                                else:
                                    lt = lt + "TMT6plex-Lys" + i.replace("TMT", "")
                            else:
                                if "N" in i or "C" in i:
                                    lt = lt + "TMT8plex-Lys" + i.replace("TMT", "") + ","
                                else:
                                    lt = lt + "TMT6plex-Lys" + i.replace("TMT", "") + ","
                    elif len(label_list) > 2:
                        for i in label_list:
                            if i == label_list[-1]:
                                lt = lt + "TMT6plex-Lys" + i.replace("TMT", "").rstrip()
                            else:
                                lt = lt + "TMT6plex-Lys" + i.replace("TMT", "").rstrip() + ","
                    else:
                        for i in label_list:
                            if i == label_list[-1]:
                                lt = lt + "TMT2plex-Lys" + i.replace("TMT", "")
                            else:
                                lt = lt + "TMT2plex-Lys" + i.replace("TMT", "") + ","

                file2label[raw] = lt

            elif row["comment[label]"].startswith("SILAC"):
                arr = sdrf[sdrf["comment[data file]"] == raw][label_cols].values
                silac_mod = file2mods[raw][1]
                for i in range(arr.shape[0]):
                    for j in range(arr.shape[1]):
                        if ":" in arr[i][j]:
                            if arr[i][j] == "SILAC light R:12C(6)14N(4)":
                                arr[i][j] = "Arg0"
                            elif arr[i][j] == "SILAC light K:12C(6)14N(2)":
                                arr[i][j] = "Lys4"
                            elif arr[i][j] == "SILAC medium R:13C(6)14N(4)":
                                arr[i][j] = "Arg6"
                            elif arr[i][j] == "SILAC medium K:13C(6)14N(4)":
                                arr[i][j] = "Lys6"
                            elif arr[i][j] == "SILAC heavy K:13C(6)15N(2)":
                                arr[i][j] = "Lys8"
                            elif arr[i][j] == "SILAC heavy R:13C(6)15N(4)":
                                arr[i][j] = "Arg10"

                        elif arr[i][j] == "SILAC heavy":
                            if "Lys8" in silac_mod:
                                arr[i][j] = "Lys8"
                            else:
                                arr[i][j] = "Arg10"
                        elif arr[i][j] == "SILAC medium":
                            if "Lys6" in silac_mod:
                                arr[i][j] = "Lys6"
                            else:
                                arr[i][j] = "Arg6"
                        elif arr[i][j] == "SILAC light":
                            if "Lys4" in silac_mod:
                                arr[i][j] = "Lys4"
                            else:
                                arr[i][j] = "Arg0"
                        elif arr[i][j] == "Unlabeled sample" or arr[i][j] == "none":
                            arr[i][j] = "Arg0"
                    if arr.shape[1] != 1:
                        for j in range(arr.shape[1]):
                            arr[i][j] = sorted(arr[i])[j]

                label_arr = np.array(list({tuple(t) for t in arr}))
                file2silac_shape[raw] = label_arr.shape
                label_arr.sort()
                if label_arr.shape[0] == 2:  # Support two or three silac labels
                    r1 = r2 = 0
                    for i in range(label_arr.shape[1]):
                        r1 = r1 + int(re.search(r"(\d+)", label_arr[0][i]).group(1))
                        r2 = r2 + int(re.search(r"(\d+)", label_arr[1][i]).group(1))
                    if r1 > r2:
                        for i in range(label_arr.shape[1]):
                            tem = label_arr[0][i]
                            label_arr[0][i] = label_arr[1][i]
                            label_arr[1][i] = tem
                    file2label[raw] = ",".join(label_arr.flatten().tolist())
                elif label_arr.shape[0] == 3:
                    r0 = r1 = r2 = 0
                    for i in range(label_arr.shape[1]):
                        r0 = r0 + int(re.search(r"(\d+)", label_arr[0][i]).group(1))
                        r1 = r1 + int(re.search(r"(\d+)", label_arr[1][i]).group(1))
                        r2 = r2 + int(re.search(r"(\d+)", label_arr[2][i]).group(1))
                    if r0 == min(r0, r1, r2):
                        if r1 == max(r0, r1, r2):
                            for i in range(label_arr.shape[1]):
                                tem = label_arr[1][i]
                                label_arr[1][i] = label_arr[2][i]
                                label_arr[2][i] = tem
                    elif r1 == min(r0, r1, r2):
                        if r0 == max(r0, r1, r2):
                            for i in range(label_arr.shape[1]):
                                tem = label_arr[0][i]
                                label_arr[0][i] = label_arr[1][i]
                                label_arr[1][i] = label_arr[2][i]
                                label_arr[2][i] = tem
                        else:
                            for i in range(label_arr.shape[1]):
                                tem = arr[0][i]
                                label_arr[0][i] = label_arr[1][i]
                                label_arr[1][i] = tem
                    elif r2 == min(r0, r1, r2):
                        if r0 == max(r0, r1, r2):
                            for i in range(label_arr.shape[1]):
                                tem = label_arr[0][i]
                                label_arr[0][i] = label_arr[2][i]
                                label_arr[2][i] = tem
                        else:
                            for i in range(label_arr.shape[1]):
                                tem = label_arr[0][i]
                                label_arr[0][i] = label_arr[2][i]
                                label_arr[2][i] = label_arr[1][i]
                                label_arr[1][i] = tem
                    file2label[raw] = ",".join(label_arr.flatten().tolist())

                else:
                    warning_message = "Only a silac label! Does it make sense?"
                    self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
                    file2label[raw] = label_arr.flatten().tolist()[0]

            elif row["comment[label]"].lower().startswith("itraq"):
                lt = ""
                label_list = sorted(list(sdrf[sdrf["comment[data file]"] == raw]["comment[label]"]))
                label_head = [re.search(r"iTRAQ(\d+)plex", i).group(1) for i in file2mods[raw] if "iTRAQ" in i]
                if len(label_head) > 0 and int(label_head[0]) >= len(label_list):
                    label_head = "iTRAQ" + label_head[0] + "plex"
                    for i in label_list:
                        if i == label_list[-1]:
                            lt = lt + label_head + "-Lys" + i.replace("iTRAQ", "").replace("reagent ", "")
                        else:
                            lt = lt + label_head + "-Lys" + i.replace("iTRAQ", "").replace("reagent ", "") + ","
                else:
                    if len(label_list) <= 4:
                        for i in label_list:
                            if i == label_list[-1]:
                                lt = lt + "iTRAQ4plex-Lys" + i.replace("iTRAQ", "").replace("reagent ", "")
                            else:
                                lt = lt + "iTRAQ4plex-Lys" + i.replace("iTRAQ", "").replace("reagent ", "") + ","
                    else:
                        for i in label_list:
                            if i == label_list[-1]:
                                lt = lt + "iTRAQ8plex-Lys" + i.replace("iTRAQ", "").replace("reagent ", "")
                            else:
                                lt = lt + "iTRAQ8plex-Lys" + i.replace("iTRAQ", "").replace("reagent ", "") + ","

                file2label[raw] = lt

            # Reading data analysis parameters
            for p in datparams.keys():
                comment_p = "comment[" + p + "]"
                if comment_p in row:
                    file2params[datparams[p]][raw] = row[comment_p]

        # create maxquant parameters xml file
        doc = Document()

        # create default textnode:Empty
        Empty_text = doc.createTextNode("")
        # create a root node
        root = doc.createElement("MaxQuantParams")
        root.setAttribute("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        doc.appendChild(root)

        # create fastaFiles subnode
        fastaFiles = doc.createElement("fastaFiles")
        FastaFileInfo = doc.createElement("FastaFileInfo")
        fastaFilePath_node = doc.createElement("fastaFilePath")
        fastaFilePath_node.appendChild(doc.createTextNode(fastaFilePath))
        identifierParseRule = doc.createElement("identifierParseRule")
        identifierParseRule.appendChild(doc.createTextNode(r">([^\s]*)"))
        descriptionParseRule = doc.createElement("descriptionParseRule")
        descriptionParseRule.appendChild(doc.createTextNode(">(.*)"))
        taxonomyParseRule = doc.createElement("taxonomyParseRule")
        taxonomyParseRule.appendChild(doc.createTextNode(""))
        variationParseRule = doc.createElement("variationParseRule")
        variationParseRule.appendChild(doc.createTextNode(""))
        modificationParseRule = doc.createElement("modificationParseRule")
        modificationParseRule.appendChild(doc.createTextNode(""))
        taxonomyId = doc.createElement("taxonomyId")
        taxonomyId.appendChild(doc.createTextNode(""))
        FastaFileInfo.appendChild(fastaFilePath_node)
        FastaFileInfo.appendChild(identifierParseRule)
        FastaFileInfo.appendChild(descriptionParseRule)
        FastaFileInfo.appendChild(taxonomyParseRule)
        FastaFileInfo.appendChild(variationParseRule)
        FastaFileInfo.appendChild(modificationParseRule)
        FastaFileInfo.appendChild(taxonomyId)
        fastaFiles.appendChild(FastaFileInfo)
        root.appendChild(fastaFiles)

        # create fastaFilesProteogenomics subnode
        fastaFilesProteogenomics = doc.createElement("fastaFilesProteogenomics")
        fastaFilesProteogenomics.appendChild(Empty_text)
        root.appendChild(fastaFilesProteogenomics)

        # create fastaFilesFirstSearch subnode
        fastaFilesFirstSearch = doc.createElement("fastaFilesFirstSearch")
        fastaFilesFirstSearch.appendChild(doc.createTextNode(""))
        root.appendChild(fastaFilesFirstSearch)

        # create fixedSearchFolder subnode
        fixedSearchFolder = doc.createElement("fixedSearchFolder")
        fixedSearchFolder.appendChild(doc.createTextNode(""))
        root.appendChild(fixedSearchFolder)

        # create andromedaCacheSize subnode
        andromedaCacheSize = doc.createElement("andromedaCacheSize")
        andromedaCacheSize.appendChild(doc.createTextNode("350000"))  # default value
        root.appendChild(andromedaCacheSize)

        # create advancedRatios subnode
        advancedRatios = doc.createElement("advancedRatios")
        advancedRatios.appendChild(doc.createTextNode("True"))
        root.appendChild(advancedRatios)

        # create pvalThres subnode
        pvalThres = doc.createElement("pvalThres")
        pvalThres.appendChild(doc.createTextNode("0.005"))
        root.appendChild(pvalThres)

        # create neucodeRatioBasedQuantification subnode
        neucodeRatioBasedQuantification = doc.createElement("neucodeRatioBasedQuantification")
        neucodeRatioBasedQuantification.appendChild(doc.createTextNode("False"))
        root.appendChild(neucodeRatioBasedQuantification)

        # create neucodeStabilizeLargeRatios subnode
        neucodeStabilizeLargeRatios = doc.createElement("neucodeStabilizeLargeRatios")
        neucodeStabilizeLargeRatios.appendChild(doc.createTextNode("False"))
        root.appendChild(neucodeStabilizeLargeRatios)

        # create rtShift subnode
        rtShift = doc.createElement("rtShift")
        rtShift.appendChild(doc.createTextNode("False"))
        root.appendChild(rtShift)

        # create some paramas and set default value
        separateLfq = doc.createElement("separateLfq")
        separateLfq.appendChild(doc.createTextNode("False"))
        root.appendChild(separateLfq)

        lfqStabilizeLargeRatios = doc.createElement("lfqStabilizeLargeRatios")
        lfqStabilizeLargeRatios.appendChild(doc.createTextNode("True"))
        root.appendChild(lfqStabilizeLargeRatios)

        lfqRequireMsms = doc.createElement("lfqRequireMsms")
        lfqRequireMsms.appendChild(doc.createTextNode("True"))
        root.appendChild(lfqRequireMsms)

        decoyMode = doc.createElement("decoyMode")
        decoyMode.appendChild(doc.createTextNode("revert"))
        root.appendChild(decoyMode)

        boxCarMode = doc.createElement("boxCarMode")
        boxCarMode.appendChild(doc.createTextNode("all"))
        root.appendChild(boxCarMode)

        includeContaminants = doc.createElement("includeContaminants")
        includeContaminants.appendChild(doc.createTextNode("True"))
        root.appendChild(includeContaminants)

        maxPeptideMass = doc.createElement("maxPeptideMass")
        maxPeptideMass.appendChild(doc.createTextNode("4600"))
        root.appendChild(maxPeptideMass)

        epsilonMutationScore = doc.createElement("epsilonMutationScore")
        epsilonMutationScore.appendChild(doc.createTextNode("True"))
        root.appendChild(epsilonMutationScore)

        mutatedPeptidesSeparately = doc.createElement("mutatedPeptidesSeparately")
        mutatedPeptidesSeparately.appendChild(doc.createTextNode("True"))
        root.appendChild(mutatedPeptidesSeparately)

        proteogenomicPeptidesSeparately = doc.createElement("proteogenomicPeptidesSeparately")
        proteogenomicPeptidesSeparately.appendChild(doc.createTextNode("True"))
        root.appendChild(proteogenomicPeptidesSeparately)

        minDeltaScoreUnmodifiedPeptides = doc.createElement("minDeltaScoreUnmodifiedPeptides")
        minDeltaScoreUnmodifiedPeptides.appendChild(doc.createTextNode("0"))
        root.appendChild(minDeltaScoreUnmodifiedPeptides)

        minDeltaScoreModifiedPeptides = doc.createElement("minDeltaScoreModifiedPeptides")
        minDeltaScoreModifiedPeptides.appendChild(doc.createTextNode("6"))
        root.appendChild(minDeltaScoreModifiedPeptides)

        minScoreUnmodifiedPeptides = doc.createElement("minScoreUnmodifiedPeptides")
        minScoreUnmodifiedPeptides.appendChild(doc.createTextNode("0"))
        root.appendChild(minScoreUnmodifiedPeptides)

        minScoreModifiedPeptides = doc.createElement("minScoreModifiedPeptides")
        minScoreModifiedPeptides.appendChild(doc.createTextNode("40"))
        root.appendChild(minScoreModifiedPeptides)

        secondPeptide = doc.createElement("secondPeptide")
        secondPeptide.appendChild(doc.createTextNode("True"))
        root.appendChild(secondPeptide)

        matchBetweenRuns_node = doc.createElement("matchBetweenRuns")
        if "enable_match_between_runs" in file2params and len(file2params["enable_match_between_runs"]) > 0:
            first = list(file2params["enable_match_between_runs"].values())[0]
            matchBetweenRuns = True
            matchBetweenRuns_node.appendChild(doc.createTextNode(first))
            warning_message = "overwriting matchBetweenRuns using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            if len(set(file2params["enable_match_between_runs"].values())) > 1:
                warning_message = "multiple values for match between runs, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            matchBetweenRuns_node.appendChild(doc.createTextNode(matchBetweenRuns))
        root.appendChild(matchBetweenRuns_node)

        matchUnidentifiedFeatures = doc.createElement("matchUnidentifiedFeatures")
        matchUnidentifiedFeatures.appendChild(doc.createTextNode("False"))
        root.appendChild(matchUnidentifiedFeatures)

        matchBetweenRunsFdr = doc.createElement("matchBetweenRunsFdr")
        matchBetweenRunsFdr.appendChild(doc.createTextNode("False"))
        root.appendChild(matchBetweenRunsFdr)

        dependentPeptides = doc.createElement("dependentPeptides")
        dependentPeptides.appendChild(doc.createTextNode("False"))
        root.appendChild(dependentPeptides)

        dependentPeptideFdr = doc.createElement("dependentPeptideFdr")
        dependentPeptideFdr.appendChild(doc.createTextNode("0"))
        root.appendChild(dependentPeptideFdr)

        dependentPeptideMassBin = doc.createElement("dependentPeptideMassBin")
        dependentPeptideMassBin.appendChild(doc.createTextNode("0"))
        root.appendChild(dependentPeptideMassBin)

        dependentPeptidesBetweenRuns = doc.createElement("dependentPeptidesBetweenRuns")
        dependentPeptidesBetweenRuns.appendChild(doc.createTextNode("False"))
        root.appendChild(dependentPeptidesBetweenRuns)

        dependentPeptidesWithinExperiment = doc.createElement("dependentPeptidesWithinExperiment")
        dependentPeptidesWithinExperiment.appendChild(doc.createTextNode("False"))
        root.appendChild(dependentPeptidesWithinExperiment)

        dependentPeptidesWithinParameterGroup = doc.createElement("dependentPeptidesWithinParameterGroup")
        dependentPeptidesWithinParameterGroup.appendChild(doc.createTextNode("False"))
        root.appendChild(dependentPeptidesWithinParameterGroup)

        dependentPeptidesRestrictFractions = doc.createElement("dependentPeptidesRestrictFractions")
        dependentPeptidesRestrictFractions.appendChild(doc.createTextNode("False"))
        root.appendChild(dependentPeptidesRestrictFractions)

        dependentPeptidesFractionDifference = doc.createElement("dependentPeptidesFractionDifference")
        dependentPeptidesFractionDifference.appendChild(doc.createTextNode("0"))
        root.appendChild(dependentPeptidesFractionDifference)

        msmsConnection = doc.createElement("msmsConnection")
        msmsConnection.appendChild(doc.createTextNode("False"))
        root.appendChild(msmsConnection)

        ibaq = doc.createElement("ibaq")
        if list(set(file2label.values())) == ["iBAQ"]:
            ibaq.appendChild(doc.createTextNode("True"))
        else:
            ibaq.appendChild(doc.createTextNode("False"))
        root.appendChild(ibaq)

        top3 = doc.createElement("top3")
        top3.appendChild(doc.createTextNode("False"))
        root.appendChild(top3)

        independentEnzymes = doc.createElement("independentEnzymes")
        independentEnzymes.appendChild(doc.createTextNode("False"))
        root.appendChild(independentEnzymes)

        useDeltaScore = doc.createElement("useDeltaScore")
        useDeltaScore.appendChild(doc.createTextNode("False"))
        root.appendChild(useDeltaScore)

        splitProteinGroupsByTaxonomy = doc.createElement("splitProteinGroupsByTaxonomy")
        splitProteinGroupsByTaxonomy.appendChild(doc.createTextNode("False"))
        root.appendChild(splitProteinGroupsByTaxonomy)

        taxonomyLevel = doc.createElement("taxonomyLevel")
        taxonomyLevel.appendChild(doc.createTextNode("Species"))
        root.appendChild(taxonomyLevel)

        avalon = doc.createElement("avalon")
        avalon.appendChild(doc.createTextNode("False"))
        root.appendChild(avalon)

        nModColumns = doc.createElement("nModColumns")
        nModColumns.appendChild(doc.createTextNode("3"))
        root.appendChild(nModColumns)

        ibaqLogFit = doc.createElement("ibaqLogFit")
        ibaqLogFit.appendChild(doc.createTextNode("False"))
        root.appendChild(ibaqLogFit)

        razorProteinFdr = doc.createElement("razorProteinFdr")
        razorProteinFdr.appendChild(doc.createTextNode("True"))
        root.appendChild(razorProteinFdr)

        deNovoSequencing = doc.createElement("deNovoSequencing")
        deNovoSequencing.appendChild(doc.createTextNode("False"))
        root.appendChild(deNovoSequencing)

        deNovoVarMods = doc.createElement("deNovoVarMods")
        deNovoVarMods.appendChild(doc.createTextNode("True"))
        root.appendChild(deNovoVarMods)

        massDifferenceSearch = doc.createElement("massDifferenceSearch")
        massDifferenceSearch.appendChild(doc.createTextNode("False"))
        root.appendChild(massDifferenceSearch)

        isotopeCalc = doc.createElement("isotopeCalc")
        isotopeCalc.appendChild(doc.createTextNode("False"))
        root.appendChild(isotopeCalc)

        writePeptidesForSpectrumFile = doc.createElement("writePeptidesForSpectrumFile")
        writePeptidesForSpectrumFile.appendChild(doc.createTextNode(""))
        root.appendChild(writePeptidesForSpectrumFile)

        intensityPredictionsFile = doc.createElement("intensityPredictionsFile")
        intensityPredictionsFile.appendChild(doc.createTextNode(""))
        root.appendChild(intensityPredictionsFile)

        minPepLen = doc.createElement("minPepLen")
        if "min_peptide_length" in file2params and len(file2params["min_peptide_length"]) > 0:
            tparam = file2params["min_peptide_length"]
            first = list(tparam.values())[0]
            minPepLen.appendChild(doc.createTextNode(first))
            warning_message = "overwriting minPepLen using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            if len(set(tparam.values())) > 1:
                warning_message = "multiple values for parameter minimum peptide length, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            minPepLen.appendChild(doc.createTextNode("7"))
        root.appendChild(minPepLen)

        psmFdrCrosslink = doc.createElement("psmFdrCrosslink")
        psmFdrCrosslink.appendChild(doc.createTextNode("0.01"))
        root.appendChild(psmFdrCrosslink)

        peptideFdr = doc.createElement("peptideFdr")
        if "ident_fdr_peptide" in file2params and len(file2params["ident_fdr_peptide"]) > 0:
            tparam = file2params["ident_fdr_peptide"]
            first = list(tparam.values())[0]
            warning_message = "overwriting peptide FDR using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            peptideFdr.appendChild(doc.createTextNode(first))
            if len(set(tparam.values())) > 1:
                warning_message = "multiple values for parameter Peptide FDR, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            peptideFdr.appendChild(doc.createTextNode(str(peptideFDR)))
        root.appendChild(peptideFdr)

        proteinFdr = doc.createElement("proteinFdr")
        if "ident_fdr_protein" in file2params and len(file2params["ident_fdr_protein"]) > 0:
            tparam = file2params["ident_fdr_protein"]
            first = list(tparam.values())[0]
            warning_message = "overwriting protein FDR using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            proteinFdr.appendChild(doc.createTextNode(first))
            if len(set(tparam.values())) > 1:
                warning_message = "multiple values for parameter Protein FDR, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            proteinFdr.appendChild(doc.createTextNode(str(proteinFDR)))
        root.appendChild(proteinFdr)

        siteFdr = doc.createElement("siteFdr")
        if "ident_fdr_psm" in file2params and len(file2params["ident_fdr_psm"]) > 0:
            tparam = file2params["ident_fdr_psm"]
            first = list(tparam.values())[0]
            warning_message = "overwriting PSM FDR using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            siteFdr.appendChild(doc.createTextNode(first))
            if len(set(tparam.values())) > 1:
                warning_message = "multiple values for parameter PSM FDR, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            siteFdr.appendChild(doc.createTextNode("0.01"))
        root.appendChild(siteFdr)

        minPeptideLengthForUnspecificSearch = doc.createElement("minPeptideLengthForUnspecificSearch")
        minPeptideLengthForUnspecificSearch.appendChild(doc.createTextNode("8"))
        root.appendChild(minPeptideLengthForUnspecificSearch)

        maxPeptideLengthForUnspecificSearch = doc.createElement("maxPeptideLengthForUnspecificSearch")
        maxPeptideLengthForUnspecificSearch.appendChild(doc.createTextNode("25"))
        root.appendChild(maxPeptideLengthForUnspecificSearch)

        useNormRatiosForOccupancy = doc.createElement("useNormRatiosForOccupancy")
        useNormRatiosForOccupancy.appendChild(doc.createTextNode("True"))
        root.appendChild(useNormRatiosForOccupancy)

        minPeptides = doc.createElement("minPeptides")
        if "min_num_peptides" in file2params and len(file2params["min_num_peptides"]) > 0:
            tparam = file2params["min_num_peptides"]
            first = list(tparam.values())[0]
            minPeptides.appendChild(doc.createTextNode(first))
            warning_message = "overwriting minPeptides using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            if len(set(tparam.values())) > 1:
                warning_message = "multiple values for parameter minimum number of peptides, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            minPeptides.appendChild(doc.createTextNode("1"))
        root.appendChild(minPeptides)

        minRazorPeptides = doc.createElement("minRazorPeptides")
        minRazorPeptides.appendChild(doc.createTextNode("1"))
        root.appendChild(minRazorPeptides)

        minUniquePeptides = doc.createElement("minUniquePeptides")
        minUniquePeptides.appendChild(doc.createTextNode("0"))
        root.appendChild(minUniquePeptides)

        useCounterparts = doc.createElement("useCounterparts")
        useCounterparts.appendChild(doc.createTextNode("False"))
        root.appendChild(useCounterparts)

        advancedSiteIntensities = doc.createElement("advancedSiteIntensities")
        advancedSiteIntensities.appendChild(doc.createTextNode("True"))
        root.appendChild(advancedSiteIntensities)

        customProteinQuantification = doc.createElement("customProteinQuantification")
        customProteinQuantification.appendChild(doc.createTextNode("False"))
        root.appendChild(customProteinQuantification)

        customProteinQuantificationFile = doc.createElement("customProteinQuantificationFile")
        customProteinQuantificationFile.appendChild(doc.createTextNode(""))
        root.appendChild(customProteinQuantificationFile)

        minRatioCount = doc.createElement("minRatioCount")
        minRatioCount.appendChild(doc.createTextNode("2"))
        root.appendChild(minRatioCount)

        restrictProteinQuantification = doc.createElement("restrictProteinQuantification")
        restrictProteinQuantification.appendChild(doc.createTextNode("True"))
        root.appendChild(restrictProteinQuantification)

        restrictMods = doc.createElement("restrictMods")
        string01 = doc.createElement("string")
        string01.appendChild(doc.createTextNode("Oxidation (M)"))
        string02 = doc.createElement("string")
        string02.appendChild(doc.createTextNode("Acetyl (Protein N-term)"))
        restrictMods.appendChild(string01)
        restrictMods.appendChild(string02)
        root.appendChild(restrictMods)

        matchingTimeWindow = doc.createElement("matchingTimeWindow")
        matchingIonMobilityWindow = doc.createElement("matchingIonMobilityWindow")
        alignmentTimeWindow = doc.createElement("alignmentTimeWindow")
        alignmentIonMobilityWindow = doc.createElement("alignmentIonMobilityWindow")
        if matchBetweenRuns:
            matchingTimeWindow.appendChild(doc.createTextNode("0.7"))
            matchingIonMobilityWindow.appendChild(doc.createTextNode("0.05"))
            alignmentTimeWindow.appendChild(doc.createTextNode("20"))
            alignmentIonMobilityWindow.appendChild(doc.createTextNode("1"))
        else:
            matchingTimeWindow.appendChild(doc.createTextNode("0"))
            matchingIonMobilityWindow.appendChild(doc.createTextNode("0"))
            alignmentTimeWindow.appendChild(doc.createTextNode("0"))
            alignmentIonMobilityWindow.appendChild(doc.createTextNode("0"))
        root.appendChild(matchingTimeWindow)
        root.appendChild(matchingIonMobilityWindow)
        root.appendChild(alignmentTimeWindow)
        root.appendChild(alignmentIonMobilityWindow)

        numberOfCandidatesMsms = doc.createElement("numberOfCandidatesMsms")
        numberOfCandidatesMsms.appendChild(doc.createTextNode("15"))
        root.appendChild(numberOfCandidatesMsms)

        compositionPrediction = doc.createElement("compositionPrediction")
        compositionPrediction.appendChild(doc.createTextNode("0"))
        root.appendChild(compositionPrediction)

        quantMode = doc.createElement("quantMode")
        if "protein_inference" in file2params and len(file2params["protein_inference"]) > 0:
            tparam = file2params["protein_inference"]
            first = list(tparam.values())[0]
            if first == "unique":
                first = "2"
            elif first == "shared":
                first = "0"
            else:
                first = "1"
            quantMode.appendChild(doc.createTextNode(first))
            warning_message = "overwriting quantMode using the value in the sdrf file"
            self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
            if len(set(tparam.values())) > 1:
                warning_message = "multiple values for parameter Quantification mode, taking the first: " + first
                self.warnings[warning_message] = self.warnings.get(warning_message, 0) + 1
        else:
            quantMode.appendChild(doc.createTextNode("1"))
        root.appendChild(quantMode)

        massDifferenceMods = doc.createElement("massDifferenceMods")
        massDifferenceMods.appendChild(doc.createTextNode(""))
        root.appendChild(massDifferenceMods)

        mainSearchMaxCombinations = doc.createElement("mainSearchMaxCombinations")
        mainSearchMaxCombinations.appendChild(doc.createTextNode("200"))
        root.appendChild(mainSearchMaxCombinations)

        writeMsScansTable = doc.createElement("writeMsScansTable")
        writeMsScansTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writeMsScansTable)

        writeMsmsScansTable = doc.createElement("writeMsmsScansTable")
        writeMsmsScansTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writeMsmsScansTable)

        writePasefMsmsScansTable = doc.createElement("writePasefMsmsScansTable")
        writePasefMsmsScansTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writePasefMsmsScansTable)

        writeAccumulatedPasefMsmsScansTable = doc.createElement("writeAccumulatedPasefMsmsScansTable")
        writeAccumulatedPasefMsmsScansTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writeAccumulatedPasefMsmsScansTable)

        writeMs3ScansTable = doc.createElement("writeMs3ScansTable")
        writeMs3ScansTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writeMs3ScansTable)

        writeAllPeptidesTable = doc.createElement("writeAllPeptidesTable")
        writeAllPeptidesTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writeAllPeptidesTable)

        writeMzRangeTable = doc.createElement("writeMzRangeTable")
        writeMzRangeTable.appendChild(doc.createTextNode("True"))
        root.appendChild(writeMzRangeTable)

        writeMzTab = doc.createElement("writeMzTab")
        writeMzTab.appendChild(doc.createTextNode("True"))
        root.appendChild(writeMzTab)

        disableMd5 = doc.createElement("disableMd5")
        disableMd5.appendChild(doc.createTextNode("False"))
        root.appendChild(disableMd5)

        cacheBinInds = doc.createElement("cacheBinInds")
        cacheBinInds.appendChild(doc.createTextNode("True"))
        root.appendChild(cacheBinInds)

        etdIncludeB = doc.createElement("etdIncludeB")
        etdIncludeB.appendChild(doc.createTextNode("False"))
        root.appendChild(etdIncludeB)

        ms2PrecursorShift = doc.createElement("ms2PrecursorShift")
        ms2PrecursorShift.appendChild(doc.createTextNode("0"))
        root.appendChild(ms2PrecursorShift)

        complementaryIonPpm = doc.createElement("complementaryIonPpm")
        complementaryIonPpm.appendChild(doc.createTextNode("20"))
        root.appendChild(complementaryIonPpm)

        variationParseRule = doc.createElement("variationParseRule")
        variationParseRule.appendChild(doc.createTextNode(""))
        root.appendChild(variationParseRule)

        variationMode = doc.createElement("variationMode")
        variationMode.appendChild(doc.createTextNode("none"))
        root.appendChild(variationMode)

        useSeriesReporters = doc.createElement("useSeriesReporters")
        useSeriesReporters.appendChild(doc.createTextNode("False"))
        root.appendChild(useSeriesReporters)

        window_name = doc.createElement("name")
        window_name.appendChild(doc.createTextNode("Session1"))
        maxquant_version = doc.createElement("maxQuantVersion")
        maxquant_version.appendChild(doc.createTextNode("1.6.10.43"))  # default version
        tempFolder_node = doc.createElement("tempFolder")
        tempFolder_node.appendChild(doc.createTextNode(tempFolder))
        pluginFolder = doc.createElement("pluginFolder")
        pluginFolder.appendChild(doc.createTextNode(""))
        numThreads_node = doc.createElement("numThreads")
        numThreads_node.appendChild(doc.createTextNode(str(numThreads)))
        emailAddress = doc.createElement("emailAddress")
        emailAddress.appendChild(doc.createTextNode(""))
        smtpHost = doc.createElement("smtpHost")
        smtpHost.appendChild(doc.createTextNode(""))
        emailFromAddress = doc.createElement("emailFromAddress")
        emailFromAddress.appendChild(doc.createTextNode(""))
        fixedCombinedFolder = doc.createElement("fixedCombinedFolder")
        fixedCombinedFolder.appendChild(doc.createTextNode(""))
        fullMinMz = doc.createElement("fullMinMz")
        fullMaxMz = doc.createElement("fullMaxMz")
        fullMaxMz.appendChild(doc.createTextNode("1.79769313486232E+308"))
        fullMinMz.appendChild(doc.createTextNode("-1.79769313486232E+308"))
        sendEmail = doc.createElement("sendEmail")
        sendEmail.appendChild(doc.createTextNode("False"))
        ionCountIntensities = doc.createElement("ionCountIntensities")
        ionCountIntensities.appendChild(doc.createTextNode("False"))
        verboseColumnHeaders = doc.createElement("verboseColumnHeaders")
        verboseColumnHeaders.appendChild(doc.createTextNode("False"))
        calcPeakProperties = doc.createElement("calcPeakProperties")
        calcPeakProperties.appendChild(doc.createTextNode("False"))
        showCentroidMassDifferences = doc.createElement("showCentroidMassDifferences")
        showCentroidMassDifferences.appendChild(doc.createTextNode("False"))
        showIsotopeMassDifferences = doc.createElement("showIsotopeMassDifferences")
        showIsotopeMassDifferences.appendChild(doc.createTextNode("False"))
        useDotNetCore = doc.createElement("useDotNetCore")
        useDotNetCore.appendChild(doc.createTextNode("False"))
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

        # create raw data file path 、experients subnode,default Path = raw file name
        # technical replicates belong to different experiments otherwise, the intensities would be combined
        filePaths = doc.createElement("filePaths")
        experiments = doc.createElement("experiments")
        raw_path = self.convert_path(raw_Folder)
        for key, value in file2source.items():
            string = doc.createElement("string")
            string.appendChild(doc.createTextNode(raw_path + key))
            filePaths.appendChild(string)
            string = doc.createElement("string")
            string.appendChild(doc.createTextNode(value + "_Tr_" + file2technical_rep[key]))
            experiments.appendChild(string)
        root.appendChild(filePaths)
        root.appendChild(experiments)

        # create fractions subnode
        fractions = doc.createElement("fractions")
        ptms = doc.createElement("ptms")
        for key, value in file2fraction.items():
            short = doc.createElement("short")
            if value == 0:
                short_text = doc.createTextNode("32767")
            else:
                short_text = doc.createTextNode(value)
            short.appendChild(short_text)
            fractions.appendChild(short)

            # create PTMS subnode
            boolean = doc.createElement("boolean")
            boolean.appendChild(doc.createTextNode("False"))
            ptms.appendChild(boolean)
        root.appendChild(fractions)
        root.appendChild(ptms)

        # create paramGroupIndices subnode
        paramGroupIndices = doc.createElement("paramGroupIndices")
        parameterGroup = {}
        tag = 0
        tmp = []

        referenceChannel = doc.createElement("referenceChannel")
        for key1, instr_val in file2instrument.items():
            value2 = (
                str(file2enzyme[key1]) + file2label[key1] + str(file2mods[key1]) + str(file2pctol) + str(file2fragtol)
            )
            datanalysisparams = {}
            for p in file2params.keys():
                if len(file2params[p]) > 0:
                    datanalysisparams[p] = file2params[p][key1]

            if tag == 0 and tmp == []:
                int_node = doc.createElement("int")
                int_node.appendChild(doc.createTextNode("0"))
                tmp.append({instr_val: value2})
                parameterGroup["0"] = {
                    "instrument": file2instrument[key1],
                    "label": file2label[key1],
                    "mods": file2mods[key1],
                    "enzyme": file2enzyme[key1],
                    "pctol": file2pctol[key1],
                    "fragtol": file2fragtol[key1],
                    "pctolunit": file2pctolunit[key1],
                    "fragtolunit": file2fragtolunit[key1],
                    "datanalysisparams": datanalysisparams,
                }
                if (
                    "Lys8" in file2label[key1]
                    or "Arg10" in file2label[key1]
                    or "Arg6" in file2label[key1]
                    or "Lys6" in file2label[key1]
                ):
                    parameterGroup["0"]["silac_shape"] = file2silac_shape[key1]

            elif {instr_val: value2} in tmp:
                int_node = doc.createElement("int")
                int_text = doc.createTextNode(str(tag))
                int_node.appendChild(int_text)

            else:
                tag += 1
                int_node = doc.createElement("int")
                int_text = doc.createTextNode(str(tag))
                int_node.appendChild(int_text)
                tmp.append({instr_val: value2})
                parameterGroup[str(tag)] = {
                    "instrument": file2instrument[key1],
                    "label": file2label[key1],
                    "mods": file2mods[key1],
                    "enzyme": file2enzyme[key1],
                    "pctol": file2pctol[key1],
                    "fragtol": file2fragtol[key1],
                    "pctolunit": file2pctolunit[key1],
                    "fragtolunit": file2fragtolunit[key1],
                    "datparams": datparams,
                }

                if (
                    "Lys8" in file2label[key1]
                    or "Arg10" in file2label[key1]
                    or "Arg6" in file2label[key1]
                    or "Lys6" in file2label[key1]
                ):
                    parameterGroup[str(tag)]["silac_shape"] = file2silac_shape[key1]

            paramGroupIndices.appendChild(int_node)

            # create referenceChannelsubnode

            string = doc.createElement("string")
            string.appendChild(doc.createTextNode(""))
            referenceChannel.appendChild(string)
        del tmp
        root.appendChild(paramGroupIndices)

        root.appendChild(referenceChannel)

        intensPred_node = doc.createElement("intensPred")
        intensPred_node.appendChild(doc.createTextNode("False"))
        root.appendChild(intensPred_node)

        intensPredModelReTrain = doc.createElement("intensPredModelReTrain")
        intensPredModelReTrain.appendChild(doc.createTextNode("False"))
        root.appendChild(intensPredModelReTrain)

        # create parameterGroup paramas subnode
        parameterGroups = doc.createElement("parameterGroups")
        for i, j in parameterGroup.items():
            parameterGroup = doc.createElement("parameterGroup")
            msInstrument = doc.createElement("msInstrument")
            if "Bruker Q-TOF" == j["instrument"]:
                msInstrument.appendChild(doc.createTextNode("1"))
                maxCharge = doc.createElement("maxCharge")
                if "max_precursor_charge" in datanalysisparams:
                    maxCharge.appendChild(doc.createTextNode(datanalysisparams["max_precursor_charge"]))
                else:
                    maxCharge.appendChild(doc.createTextNode("5"))
                minPeakLen = doc.createElement("minPeakLen")
                minPeakLen.appendChild(doc.createTextNode("3"))
                diaMinPeakLen = doc.createElement("diaMinPeakLen")
                diaMinPeakLen.appendChild(doc.createTextNode("3"))
                useMs1Centroids = doc.createElement("useMs1Centroids")
                useMs1Centroids.appendChild(doc.createTextNode("True"))
                useMs2Centroids = doc.createElement("useMs2Centroids")
                useMs2Centroids.appendChild(doc.createTextNode("True"))
                intensityDetermination = doc.createElement("intensityDetermination")
                intensityDetermination.appendChild(doc.createTextNode("0"))
                centroidMatchTol = doc.createElement("centroidMatchTol")
                centroidMatchTol.appendChild(doc.createTextNode("0.008"))
                centroidMatchTolInPpm = doc.createElement("centroidMatchTolInPpm")
                centroidMatchTolInPpm.appendChild(doc.createTextNode("True"))
                valleyFactor = doc.createElement("valleyFactor")
                valleyFactor.appendChild(doc.createTextNode("1.2"))
                advancedPeakSplitting = doc.createElement("advancedPeakSplitting")
                advancedPeakSplitting.appendChild(doc.createTextNode("True"))
                intensityThreshold = doc.createElement("intensityThreshold")
                intensityThreshold.appendChild(doc.createTextNode("30"))
            elif "AB Sciex Q-TOF" == j["instrument"]:
                msInstrument.appendChild(doc.createTextNode("2"))
                maxCharge = doc.createElement("maxCharge")
                maxCharge.appendChild(doc.createTextNode("5"))
                minPeakLen = doc.createElement("minPeakLen")
                minPeakLen.appendChild(doc.createTextNode("3"))
                diaMinPeakLen = doc.createElement("diaMinPeakLen")
                diaMinPeakLen.appendChild(doc.createTextNode("3"))
                useMs1Centroids = doc.createElement("useMs1Centroids")
                useMs1Centroids.appendChild(doc.createTextNode("True"))
                useMs2Centroids = doc.createElement("useMs2Centroids")
                useMs2Centroids.appendChild(doc.createTextNode("True"))
                intensityDetermination = doc.createElement("intensityDetermination")
                intensityDetermination.appendChild(doc.createTextNode("0"))
                centroidMatchTol = doc.createElement("centroidMatchTol")
                centroidMatchTol.appendChild(doc.createTextNode("0.01"))
                centroidMatchTolInPpm = doc.createElement("centroidMatchTolInPpm")
                centroidMatchTolInPpm.appendChild(doc.createTextNode("True"))
                valleyFactor = doc.createElement("valleyFactor")
                valleyFactor.appendChild(doc.createTextNode("1.2"))
                advancedPeakSplitting = doc.createElement("advancedPeakSplitting")
                advancedPeakSplitting.appendChild(doc.createTextNode("True"))
                intensityThreshold = doc.createElement("intensityThreshold")
                intensityThreshold.appendChild(doc.createTextNode("0"))
            elif "Agilent Q-TOF" == j["instrument"]:
                msInstrument.appendChild(doc.createTextNode("3"))
                maxCharge = doc.createElement("maxCharge")
                maxCharge.appendChild(doc.createTextNode("5"))
                minPeakLen = doc.createElement("minPeakLen")
                minPeakLen.appendChild(doc.createTextNode("3"))
                diaMinPeakLen = doc.createElement("diaMinPeakLen")
                diaMinPeakLen.appendChild(doc.createTextNode("3"))
                useMs1Centroids = doc.createElement("useMs1Centroids")
                useMs1Centroids.appendChild(doc.createTextNode("True"))
                useMs2Centroids = doc.createElement("useMs2Centroids")
                useMs2Centroids.appendChild(doc.createTextNode("True"))
                intensityDetermination = doc.createElement("intensityDetermination")
                intensityDetermination.appendChild(doc.createTextNode("0"))
                centroidMatchTol = doc.createElement("centroidMatchTol")
                centroidMatchTol.appendChild(doc.createTextNode("0.008"))
                centroidMatchTolInPpm = doc.createElement("centroidMatchTolInPpm")
                centroidMatchTolInPpm.appendChild(doc.createTextNode("False"))
                valleyFactor = doc.createElement("valleyFactor")
                valleyFactor.appendChild(doc.createTextNode("1.2"))
                advancedPeakSplitting = doc.createElement("advancedPeakSplitting")
                advancedPeakSplitting.appendChild(doc.createTextNode("True"))
                intensityThreshold = doc.createElement("intensityThreshold")
                intensityThreshold.appendChild(doc.createTextNode("0"))
            elif "Bruker TIMS" == j["instrument"]:
                msInstrument.appendChild(doc.createTextNode("4"))
                maxCharge = doc.createElement("maxCharge")
                maxCharge.appendChild(doc.createTextNode("4"))
                minPeakLen = doc.createElement("minPeakLen")
                minPeakLen.appendChild(doc.createTextNode("2"))
                diaMinPeakLen = doc.createElement("diaMinPeakLen")
                diaMinPeakLen.appendChild(doc.createTextNode("2"))
                useMs1Centroids = doc.createElement("useMs1Centroids")
                useMs1Centroids.appendChild(doc.createTextNode("True"))
                useMs2Centroids = doc.createElement("useMs2Centroids")
                useMs2Centroids.appendChild(doc.createTextNode("True"))
                intensityDetermination = doc.createElement("intensityDetermination")
                intensityDetermination.appendChild(doc.createTextNode("0"))
                centroidMatchTol = doc.createElement("centroidMatchTol")
                centroidMatchTol.appendChild(doc.createTextNode("10"))
                centroidMatchTolInPpm = doc.createElement("centroidMatchTolInPpm")
                centroidMatchTolInPpm.appendChild(doc.createTextNode("True"))
                valleyFactor = doc.createElement("valleyFactor")
                valleyFactor.appendChild(doc.createTextNode("1.2"))
                advancedPeakSplitting = doc.createElement("advancedPeakSplitting")
                advancedPeakSplitting.appendChild(doc.createTextNode("True"))
                intensityThreshold = doc.createElement("intensityThreshold")
                intensityThreshold.appendChild(doc.createTextNode("30"))
            else:
                msInstrument.appendChild(doc.createTextNode("0"))
                maxCharge = doc.createElement("maxCharge")
                maxCharge.appendChild(doc.createTextNode("7"))
                minPeakLen = doc.createElement("minPeakLen")
                minPeakLen.appendChild(doc.createTextNode("2"))
                diaMinPeakLen = doc.createElement("diaMinPeakLen")
                diaMinPeakLen.appendChild(doc.createTextNode("2"))
                useMs1Centroids = doc.createElement("useMs1Centroids")
                useMs1Centroids.appendChild(doc.createTextNode("False"))
                useMs2Centroids = doc.createElement("useMs2Centroids")
                useMs2Centroids.appendChild(doc.createTextNode("False"))
                intensityDetermination = doc.createElement("intensityDetermination")
                intensityDetermination.appendChild(doc.createTextNode("0"))
                centroidMatchTol = doc.createElement("centroidMatchTol")
                centroidMatchTol.appendChild(doc.createTextNode("8"))
                centroidMatchTolInPpm = doc.createElement("centroidMatchTolInPpm")
                centroidMatchTolInPpm.appendChild(doc.createTextNode("True"))
                valleyFactor = doc.createElement("valleyFactor")
                valleyFactor.appendChild(doc.createTextNode("1.4"))
                advancedPeakSplitting = doc.createElement("advancedPeakSplitting")
                advancedPeakSplitting.appendChild(doc.createTextNode("False"))
                intensityThreshold = doc.createElement("intensityThreshold")
                intensityThreshold.appendChild(doc.createTextNode("0"))

            cutPeaks = doc.createElement("cutPeaks")
            cutPeaks.appendChild(doc.createTextNode("True"))
            gapScans = doc.createElement("gapScans")
            gapScans.appendChild(doc.createTextNode("1"))
            minTime = doc.createElement("minTime")
            minTime.appendChild(doc.createTextNode("NaN"))
            maxTime = doc.createElement("maxTime")
            maxTime.appendChild(doc.createTextNode("NaN"))
            matchType = doc.createElement("matchType")
            matchType.appendChild(doc.createTextNode("MatchFromAndTo"))
            centroidHalfWidth = doc.createElement("centroidHalfWidth")
            centroidHalfWidth.appendChild(doc.createTextNode("35"))
            centroidHalfWidthInPpm = doc.createElement("centroidHalfWidthInPpm")
            centroidHalfWidthInPpm.appendChild(doc.createTextNode("True"))
            isotopeValleyFactor = doc.createElement("isotopeValleyFactor")
            isotopeValleyFactor.appendChild(doc.createTextNode("1.2"))
            labelMods = doc.createElement("labelMods")
            if "Lys8" in j["label"] or "Arg10" in j["label"] or "Arg6" in j["label"] or "Lys6" in j["label"]:
                for lm in range(j["silac_shape"][0]):
                    r = j["label"].split(",")[
                        lm * j["silac_shape"][1] : lm * j["silac_shape"][1] + lm * j["silac_shape"][1]
                    ]
                    if "Arg0" in r:
                        r.remove("Arg0")
                    text = ";".join(r)
                    string = doc.createElement("string")
                    string.appendChild(doc.createTextNode(text))
                    labelMods.appendChild(string)

            else:
                string = doc.createElement("string")
                string.appendChild(doc.createTextNode(""))
                labelMods.appendChild(string)
            lcmsRunType = doc.createElement("lcmsRunType")
            if "TMT" in j["label"] or "iTRAQ" in j["label"]:
                lcmsRunType.appendChild(doc.createTextNode("Reporter ion MS2"))
            else:
                lcmsRunType.appendChild(doc.createTextNode("Standard"))
            reQuantify = doc.createElement("reQuantify")
            reQuantify.appendChild(doc.createTextNode("False"))

            # create label subnode
            lfqMode = doc.createElement("lfqMode")
            if j["label"] == "label free sample":
                lfqMode.appendChild(doc.createTextNode("1"))
            else:
                lfqMode.appendChild(doc.createTextNode("0"))

            lfqSkipNorm = doc.createElement("lfqSkipNorm")
            lfqSkipNorm.appendChild(doc.createTextNode("False"))
            lfqMinEdgesPerNode = doc.createElement("lfqMinEdgesPerNode")
            lfqMinEdgesPerNode.appendChild(doc.createTextNode("3"))
            lfqAvEdgesPerNode = doc.createElement("lfqAvEdgesPerNode")
            lfqAvEdgesPerNode.appendChild(doc.createTextNode("6"))
            lfqMaxFeatures = doc.createElement("lfqMaxFeatures")
            lfqMaxFeatures.appendChild(doc.createTextNode("100000"))
            neucodeMaxPpm = doc.createElement("neucodeMaxPpm")
            neucodeMaxPpm.appendChild(doc.createTextNode("0"))
            neucodeResolution = doc.createElement("neucodeResolution")
            neucodeResolution.appendChild(doc.createTextNode("0"))
            neucodeResolutionInMda = doc.createElement("neucodeResolutionInMda")
            neucodeResolutionInMda.appendChild(doc.createTextNode("False"))
            neucodeInSilicoLowRes = doc.createElement("neucodeInSilicoLowRes")
            neucodeInSilicoLowRes.appendChild(doc.createTextNode("False"))
            fastLfq = doc.createElement("fastLfq")
            fastLfq.appendChild(doc.createTextNode("True"))
            lfqRestrictFeatures = doc.createElement("lfqRestrictFeatures")
            lfqRestrictFeatures.appendChild(doc.createTextNode("False"))
            lfqMinRatioCount = doc.createElement("lfqMinRatioCount")
            lfqMinRatioCount.appendChild(doc.createTextNode("2"))
            maxLabeledAa = doc.createElement("maxLabeledAa")
            multiplicity = doc.createElement("multiplicity")
            if "Lys8" in j["label"] or "Arg10" in j["label"] or "Arg6" in j["label"] or "Lys6" in j["label"]:
                maxLabeledAa.appendChild(doc.createTextNode("3"))
                multiplicity.appendChild(doc.createTextNode(str(j["silac_shape"][0])))
            else:
                maxLabeledAa.appendChild(doc.createTextNode("0"))
                multiplicity.appendChild(doc.createTextNode("1"))

            maxNmods = doc.createElement("maxNmods")
            if "max_mods" in datanalysisparams:
                maxNmods.appendChild(doc.createTextNode(datanalysisparams["max_mods"]))
            else:
                maxNmods.appendChild(doc.createTextNode("5"))

            maxMissedCleavages = doc.createElement("maxMissedCleavages")
            if "allowed_miscleavages" in datanalysisparams:
                maxMissedCleavages.appendChild(doc.createTextNode(datanalysisparams["allowed_miscleavages"]))
            else:
                maxMissedCleavages.appendChild(doc.createTextNode("2"))
            enzymeMode = doc.createElement("enzymeMode")
            enzymeMode.appendChild(doc.createTextNode("0"))
            complementaryReporterType = doc.createElement("complementaryReporterType")
            complementaryReporterType.appendChild(doc.createTextNode("0"))
            reporterNormalization = doc.createElement("reporterNormalization")
            reporterNormalization.appendChild(doc.createTextNode("0"))
            neucodeIntensityMode = doc.createElement("neucodeIntensityMode")
            neucodeIntensityMode.appendChild(doc.createTextNode("0"))

            # create Modification subnode
            fixedModifications = doc.createElement("fixedModifications")
            variableModifications = doc.createElement("variableModifications")

            def parse_mods(mods):
                mods_list = []
                if mods != "":
                    mods_list.extend(mods.split(","))
                return list(set(mods_list))

            fixedM_list = parse_mods(j["mods"][0])
            Variable_list = parse_mods(j["mods"][1])

            fixedM_list.extend(j["mods"][0].split(","))
            fixedM_list = list(set(fixedM_list))
            for F in fixedM_list:
                string = doc.createElement("string")
                string.appendChild(doc.createTextNode(F))
                fixedModifications.appendChild(string)
            for V in Variable_list:
                if V in ["Lys8", "Lys6", "Lys4", "Arg10", "Arg6"]:
                    continue
                string = doc.createElement("string")
                string.appendChild(doc.createTextNode(V))
                variableModifications.appendChild(string)

            # create enzymes subnode
            enzymes_node = doc.createElement("enzymes")
            for index in range(len(j["enzyme"])):
                string = doc.createElement("string")
                string.appendChild(doc.createTextNode(j["enzyme"][index]))
                enzymes_node.appendChild(string)
            enzymesFirstSearch = doc.createElement("enzymesFirstSearch")
            enzymesFirstSearch.appendChild(doc.createTextNode(""))
            enzymeModeFirstSearch = doc.createElement("enzymeModeFirstSearch")
            enzymeModeFirstSearch.appendChild(doc.createTextNode("0"))
            useEnzymeFirstSearch = doc.createElement("useEnzymeFirstSearch")
            useEnzymeFirstSearch.appendChild(doc.createTextNode("False"))

            # create variable modification
            useVariableModificationsFirstSearch = doc.createElement("useVariableModificationsFirstSearch")
            useVariableModificationsFirstSearch.appendChild(doc.createTextNode("False"))
            useMultiModification = doc.createElement("useMultiModification")
            useMultiModification.appendChild(doc.createTextNode("False"))
            multiModifications = doc.createElement("multiModifications")
            multiModifications.appendChild(doc.createTextNode(""))
            isobaricLabels = doc.createElement("isobaricLabels")
            if "TMT" in j["label"]:
                for t in j["label"].split(","):
                    IsobaricLabelInfo = doc.createElement("IsobaricLabelInfo")
                    internalLabel = doc.createElement("internalLabel")
                    internalLabel.appendChild(doc.createTextNode(t))
                    IsobaricLabelInfo.appendChild(internalLabel)
                    terminalLabel = doc.createElement("terminalLabel")
                    terminalLabel.appendChild(doc.createTextNode(t.replace("-Lys", "-Nter")))
                    IsobaricLabelInfo.appendChild(terminalLabel)
                    correctionFactorM2 = doc.createElement("correctionFactorM2")
                    correctionFactorM2.appendChild(doc.createTextNode("0"))
                    correctionFactorM1 = doc.createElement("correctionFactorM1")
                    correctionFactorM1.appendChild(doc.createTextNode("0"))
                    correctionFactorP1 = doc.createElement("correctionFactorP1")
                    correctionFactorP1.appendChild(doc.createTextNode("0"))
                    correctionFactorP2 = doc.createElement("correctionFactorP2")
                    correctionFactorP2.appendChild(doc.createTextNode("0"))
                    tmtLike = doc.createElement("tmtLike")
                    tmtLike.appendChild(doc.createTextNode("True"))
                    IsobaricLabelInfo.appendChild(correctionFactorM2)
                    IsobaricLabelInfo.appendChild(correctionFactorM1)
                    IsobaricLabelInfo.appendChild(correctionFactorP1)
                    IsobaricLabelInfo.appendChild(correctionFactorP2)
                    IsobaricLabelInfo.appendChild(tmtLike)
                    isobaricLabels.appendChild(IsobaricLabelInfo)
            elif "iTRAQ" in j["label"]:
                for t in j["label"].split(","):
                    IsobaricLabelInfo = doc.createElement("IsobaricLabelInfo")
                    internalLabel = doc.createElement("internalLabel")
                    internalLabel.appendChild(doc.createTextNode(t))
                    IsobaricLabelInfo.appendChild(internalLabel)
                    terminalLabel = doc.createElement("terminalLabel")
                    terminalLabel.appendChild(doc.createTextNode(t.replace("-Lys", "-Nter")))
                    IsobaricLabelInfo.appendChild(terminalLabel)
                    correctionFactorM2 = doc.createElement("correctionFactorM2")
                    correctionFactorM2.appendChild(doc.createTextNode("0"))
                    correctionFactorM1 = doc.createElement("correctionFactorM1")
                    correctionFactorM1.appendChild(doc.createTextNode("0"))
                    correctionFactorP1 = doc.createElement("correctionFactorP1")
                    correctionFactorP1.appendChild(doc.createTextNode("0"))
                    correctionFactorP2 = doc.createElement("correctionFactorP2")
                    correctionFactorP2.appendChild(doc.createTextNode("0"))
                    tmtLike = doc.createElement("tmtLike")
                    tmtLike.appendChild(doc.createTextNode("False"))
                    IsobaricLabelInfo.appendChild(correctionFactorM2)
                    IsobaricLabelInfo.appendChild(correctionFactorM1)
                    IsobaricLabelInfo.appendChild(correctionFactorP1)
                    IsobaricLabelInfo.appendChild(correctionFactorP2)
                    IsobaricLabelInfo.appendChild(tmtLike)
                    isobaricLabels.appendChild(IsobaricLabelInfo)
            else:
                isobaricLabels.appendChild(doc.createTextNode(""))

            neucodeLabels = doc.createElement("neucodeLabels")
            neucodeLabels.appendChild(doc.createTextNode(""))
            variableModificationsFirstSearch = doc.createElement("variableModificationsFirstSearch")
            variableModificationsFirstSearch.appendChild(doc.createTextNode(""))
            hasAdditionalVariableModifications = doc.createElement("hasAdditionalVariableModifications")
            hasAdditionalVariableModifications.appendChild(doc.createTextNode("False"))
            additionalVariableModifications = doc.createElement("additionalVariableModifications")
            additionalVariableModifications.appendChild(doc.createTextNode(""))
            additionalVariableModificationProteins = doc.createElement("additionalVariableModificationProteins")
            additionalVariableModificationProteins.appendChild(doc.createTextNode(""))
            doMassFiltering = doc.createElement("doMassFiltering")
            doMassFiltering.appendChild(doc.createTextNode("True"))
            firstSearchTol = doc.createElement("firstSearchTol")
            mainSearchTol = doc.createElement("mainSearchTol")
            if j["pctolunit"] == "ppm":
                firstSearchTol.appendChild(doc.createTextNode(str(float(j["pctol"]) + 15)))
                mainSearchTol.appendChild(doc.createTextNode(str(j["pctol"])))
                searchTolInPpm = doc.createElement("searchTolInPpm")
                searchTolInPpm.appendChild(doc.createTextNode("True"))
            else:
                firstSearchTol.appendChild(doc.createTextNode(str(float(j["pctol"]) + 0.04)))
                mainSearchTol.appendChild(doc.createTextNode(str(j["pctol"])))
                searchTolInPpm = doc.createElement("searchTolInPpm")
                searchTolInPpm.appendChild(doc.createTextNode("False"))
            isotopeMatchTol = doc.createElement("isotopeMatchTol")

            isotopeMatchTol.appendChild(doc.createTextNode("2"))
            isotopeMatchTolInPpm = doc.createElement("isotopeMatchTolInPpm")
            isotopeMatchTolInPpm.appendChild(doc.createTextNode("True"))
            isotopeTimeCorrelation = doc.createElement("isotopeTimeCorrelation")
            isotopeTimeCorrelation.appendChild(doc.createTextNode("0.6"))
            theorIsotopeCorrelation = doc.createElement("theorIsotopeCorrelation")
            theorIsotopeCorrelation.appendChild(doc.createTextNode("0.6"))
            checkMassDeficit = doc.createElement("checkMassDeficit")
            checkMassDeficit.appendChild(doc.createTextNode("True"))
            recalibrationInPpm = doc.createElement("recalibrationInPpm")
            recalibrationInPpm.appendChild(doc.createTextNode("True"))
            intensityDependentCalibration = doc.createElement("intensityDependentCalibration")
            intensityDependentCalibration.appendChild(doc.createTextNode("False"))
            minScoreForCalibration = doc.createElement("minScoreForCalibration")
            minScoreForCalibration.appendChild(doc.createTextNode("70"))
            matchLibraryFile = doc.createElement("matchLibraryFile")
            matchLibraryFile.appendChild(doc.createTextNode("False"))
            libraryFile = doc.createElement("libraryFile")
            libraryFile.appendChild(doc.createTextNode(""))
            matchLibraryMassTolPpm = doc.createElement("matchLibraryMassTolPpm")
            matchLibraryMassTolPpm.appendChild(doc.createTextNode("0"))
            matchLibraryTimeTolMin = doc.createElement("matchLibraryTimeTolMin")
            matchLibraryTimeTolMin.appendChild(doc.createTextNode("0"))
            matchLabelTimeTolMin = doc.createElement("matchLabelTimeTolMin")
            matchLabelTimeTolMin.appendChild(doc.createTextNode("0"))
            reporterMassTolerance = doc.createElement("reporterMassTolerance")
            reporterPif = doc.createElement("reporterPif")
            filterPif = doc.createElement("filterPif")
            reporterFraction = doc.createElement("reporterFraction")
            reporterBasePeakRatio = doc.createElement("reporterBasePeakRatio")
            if "TMT" in j["label"]:
                reporterMassTolerance.appendChild(doc.createTextNode("0.003"))
                reporterPif.appendChild(doc.createTextNode("0"))
                filterPif.appendChild(doc.createTextNode("False"))
                reporterFraction.appendChild(doc.createTextNode("0"))
                reporterBasePeakRatio.appendChild(doc.createTextNode("0"))
            else:
                reporterMassTolerance.appendChild(doc.createTextNode("NaN"))
                reporterPif.appendChild(doc.createTextNode("NaN"))
                filterPif.appendChild(doc.createTextNode("False"))
                reporterFraction.appendChild(doc.createTextNode("NaN"))
                reporterBasePeakRatio.appendChild(doc.createTextNode("NaN"))
            timsHalfWidth = doc.createElement("timsHalfWidth")
            timsHalfWidth.appendChild(doc.createTextNode("0"))
            timsStep = doc.createElement("timsStep")
            timsStep.appendChild(doc.createTextNode("0"))
            timsResolution = doc.createElement("timsResolution")
            timsResolution.appendChild(doc.createTextNode("0"))
            timsMinMsmsIntensity = doc.createElement("timsMinMsmsIntensity")
            timsMinMsmsIntensity.appendChild(doc.createTextNode("0"))
            timsRemovePrecursor = doc.createElement("timsRemovePrecursor")
            timsRemovePrecursor.appendChild(doc.createTextNode("True"))
            timsIsobaricLabels = doc.createElement("timsIsobaricLabels")
            timsIsobaricLabels.appendChild(doc.createTextNode("False"))
            timsCollapseMsms = doc.createElement("timsCollapseMsms")
            timsCollapseMsms.appendChild(doc.createTextNode("True"))
            crosslinkSearch = doc.createElement("crosslinkSearch")
            crosslinkSearch.appendChild(doc.createTextNode("False"))
            crossLinker = doc.createElement("crossLinker")
            crossLinker.appendChild(doc.createTextNode(""))
            minMatchXl = doc.createElement("minMatchXl")
            minMatchXl.appendChild(doc.createTextNode("0"))
            minPairedPepLenXl = doc.createElement("minPairedPepLenXl")
            minPairedPepLenXl.appendChild(doc.createTextNode("6"))
            crosslinkOnlyIntraProtein = doc.createElement("crosslinkOnlyIntraProtein")
            crosslinkOnlyIntraProtein.appendChild(doc.createTextNode("False"))
            crosslinkMaxMonoUnsaturated = doc.createElement("crosslinkMaxMonoUnsaturated")
            crosslinkMaxMonoUnsaturated.appendChild(doc.createTextNode("0"))
            crosslinkMaxMonoSaturated = doc.createElement("crosslinkMaxMonoSaturated")
            crosslinkMaxMonoSaturated.appendChild(doc.createTextNode("0"))
            crosslinkMaxDiUnsaturated = doc.createElement("crosslinkMaxDiUnsaturated")
            crosslinkMaxDiUnsaturated.appendChild(doc.createTextNode("0"))
            crosslinkMaxDiSaturated = doc.createElement("crosslinkMaxDiSaturated")
            crosslinkMaxDiSaturated.appendChild(doc.createTextNode("0"))

            crosslinkModifications = doc.createElement("crosslinkModifications")
            crosslinkModifications.appendChild(doc.createTextNode(""))
            crosslinkFastaFiles = doc.createElement("crosslinkFastaFiles")
            crosslinkFastaFiles.appendChild(doc.createTextNode(""))
            crosslinkSites = doc.createElement("crosslinkSites")
            crosslinkSites.appendChild(doc.createTextNode(""))
            crosslinkNetworkFiles = doc.createElement("crosslinkNetworkFiles")
            crosslinkNetworkFiles.appendChild(doc.createTextNode(""))

            crosslinkMode = doc.createElement("crosslinkMode")
            crosslinkMode.appendChild(doc.createTextNode("PeptidesWithCleavedLinker"))
            peakRefinement = doc.createElement("peakRefinement")
            peakRefinement.appendChild(doc.createTextNode("False"))
            isobaricSumOverWindow = doc.createElement("isobaricSumOverWindow")
            isobaricSumOverWindow.appendChild(doc.createTextNode("True"))
            isobaricWeightExponent = doc.createElement("isobaricWeightExponent")
            isobaricWeightExponent.appendChild(doc.createTextNode("0.75"))
            diaLibraryType = doc.createElement("diaLibraryType")
            diaLibraryType.appendChild(doc.createTextNode("0"))
            diaLibraryPath = doc.createElement("diaLibraryPath")
            diaLibraryPath.appendChild(doc.createTextNode(""))
            diaPeptidePaths = doc.createElement("diaPeptidePaths")
            diaPeptidePaths.appendChild(doc.createTextNode(""))
            diaEvidencePaths = doc.createElement("diaEvidencePaths")
            diaEvidencePaths.appendChild(doc.createTextNode(""))
            diaMsmsPaths = doc.createElement("diaMsmsPaths")
            diaMsmsPaths.appendChild(doc.createTextNode(""))
            diaInitialPrecMassTolPpm = doc.createElement("diaInitialPrecMassTolPpm")
            diaInitialPrecMassTolPpm.appendChild(doc.createTextNode("20"))
            diaInitialFragMassTolPpm = doc.createElement("diaInitialFragMassTolPpm")
            diaInitialFragMassTolPpm.appendChild(doc.createTextNode("20"))
            diaCorrThresholdFeatureClustering = doc.createElement("diaCorrThresholdFeatureClustering")
            diaCorrThresholdFeatureClustering.appendChild(doc.createTextNode("0.85"))
            diaPrecTolPpmFeatureClustering = doc.createElement("diaPrecTolPpmFeatureClustering")
            diaPrecTolPpmFeatureClustering.appendChild(doc.createTextNode("2"))
            diaFragTolPpmFeatureClustering = doc.createElement("diaFragTolPpmFeatureClustering")
            diaFragTolPpmFeatureClustering.appendChild(doc.createTextNode("2"))
            diaScoreN = doc.createElement("diaScoreN")
            diaScoreN.appendChild(doc.createTextNode("7"))
            diaMinScore = doc.createElement("diaMinScore")
            diaMinScore.appendChild(doc.createTextNode("2.99"))
            diaPrecursorQuant = doc.createElement("diaPrecursorQuant")
            diaPrecursorQuant.appendChild(doc.createTextNode("False"))
            diaDiaTopNFragmentsForQuant = doc.createElement("diaDiaTopNFragmentsForQuant")
            diaDiaTopNFragmentsForQuant.appendChild(doc.createTextNode("3"))

            # appendChild in parameterGroup
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
            # parameterGroup.appendChild(crosslinkUseSeparateFasta)
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

        # create msmsParamsArray subnode
        msmsParamsArray = doc.createElement("msmsParamsArray")
        for i in range(4):
            msmsParams = doc.createElement("msmsParams")
            Name = doc.createElement("Name")
            MatchTolerance = doc.createElement("MatchTolerance")
            MatchToleranceInPpm = doc.createElement("MatchToleranceInPpm")
            MatchTolerance.appendChild(doc.createTextNode(str(j["fragtol"])))
            if j["fragtolunit"] == "ppm":
                MatchToleranceInPpm.appendChild(doc.createTextNode("True"))
            else:
                MatchToleranceInPpm.appendChild(doc.createTextNode("False"))
            DeisotopeTolerance = doc.createElement("DeisotopeTolerance")
            DeisotopeToleranceInPpm = doc.createElement("DeisotopeToleranceInPpm")
            DeNovoTolerance = doc.createElement("DeNovoTolerance")
            DeNovoToleranceInPpm = doc.createElement("DeNovoToleranceInPpm")
            Deisotope = doc.createElement("Deisotope")
            Topx = doc.createElement("Topx")
            TopxInterval = doc.createElement("TopxInterval")
            HigherCharges = doc.createElement("HigherCharges")
            IncludeWater = doc.createElement("IncludeWater")
            IncludeAmmonia = doc.createElement("IncludeAmmonia")
            DependentLosses = doc.createElement("DependentLosses")
            Recalibration = doc.createElement("Recalibration")

            if i == 0:
                Name.appendChild(doc.createTextNode("FTMS"))
                DeisotopeTolerance.appendChild(doc.createTextNode("7"))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode("True"))
                DeNovoTolerance.appendChild(doc.createTextNode("10"))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode("True"))
                Deisotope.appendChild(doc.createTextNode("True"))
                Topx.appendChild(doc.createTextNode("12"))
                TopxInterval.appendChild(doc.createTextNode("100"))
                HigherCharges.appendChild(doc.createTextNode("True"))
                IncludeWater.appendChild(doc.createTextNode("True"))
                IncludeAmmonia.appendChild(doc.createTextNode("True"))
                DependentLosses.appendChild(doc.createTextNode("True"))
                Recalibration.appendChild(doc.createTextNode("False"))

            elif i == 1:
                Name.appendChild(doc.createTextNode("ITMS"))
                DeisotopeTolerance.appendChild(doc.createTextNode("0.15"))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode("False"))
                DeNovoTolerance.appendChild(doc.createTextNode("0.25"))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode("False"))
                Deisotope.appendChild(doc.createTextNode("False"))
                Topx.appendChild(doc.createTextNode("8"))
                TopxInterval.appendChild(doc.createTextNode("100"))
                HigherCharges.appendChild(doc.createTextNode("True"))
                IncludeWater.appendChild(doc.createTextNode("True"))
                IncludeAmmonia.appendChild(doc.createTextNode("True"))
                DependentLosses.appendChild(doc.createTextNode("True"))
                Recalibration.appendChild(doc.createTextNode("False"))

            elif i == 2:
                Name.appendChild(doc.createTextNode("TOF"))
                DeisotopeTolerance.appendChild(doc.createTextNode("0.01"))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode("False"))
                DeNovoTolerance.appendChild(doc.createTextNode("0.02"))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode("False"))
                Deisotope.appendChild(doc.createTextNode("True"))
                Topx.appendChild(doc.createTextNode("10"))
                TopxInterval.appendChild(doc.createTextNode("100"))
                HigherCharges.appendChild(doc.createTextNode("True"))
                IncludeWater.appendChild(doc.createTextNode("True"))
                IncludeAmmonia.appendChild(doc.createTextNode("True"))
                DependentLosses.appendChild(doc.createTextNode("True"))
                Recalibration.appendChild(doc.createTextNode("False"))

            elif i == 3:
                Name.appendChild(doc.createTextNode("Unknown"))
                DeisotopeTolerance.appendChild(doc.createTextNode("7"))
                DeisotopeToleranceInPpm.appendChild(doc.createTextNode("True"))
                DeNovoTolerance.appendChild(doc.createTextNode("10"))
                DeNovoToleranceInPpm.appendChild(doc.createTextNode("True"))
                Deisotope.appendChild(doc.createTextNode("True"))
                Topx.appendChild(doc.createTextNode("12"))
                TopxInterval.appendChild(doc.createTextNode("100"))
                HigherCharges.appendChild(doc.createTextNode("True"))
                IncludeWater.appendChild(doc.createTextNode("True"))
                IncludeAmmonia.appendChild(doc.createTextNode("True"))
                DependentLosses.appendChild(doc.createTextNode("True"))
                Recalibration.appendChild(doc.createTextNode("False"))

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

        # create fragmentationParamsArray subnode
        fragmentationParamsArray = doc.createElement("fragmentationParamsArray")
        for i in ["CID", "HCD", "ETD", "PQD", "ETHCD", "ETCID", "UVPD", "Unknown"]:
            fragmentationParams = doc.createElement("fragmentationParams")
            fragment_Name = doc.createElement("Name")
            fragment_Name.appendChild(doc.createTextNode(i))
            Connected = doc.createElement("Connected")
            Connected.appendChild(doc.createTextNode("False"))
            ConnectedScore0 = doc.createElement("ConnectedScore0")
            ConnectedScore0.appendChild(doc.createTextNode("1"))
            ConnectedScore1 = doc.createElement("ConnectedScore1")
            ConnectedScore1.appendChild(doc.createTextNode("1"))
            ConnectedScore2 = doc.createElement("ConnectedScore2")
            ConnectedScore2.appendChild(doc.createTextNode("1"))
            InternalFragments = doc.createElement("InternalFragments")
            InternalFragments.appendChild(doc.createTextNode("False"))
            InternalFragmentWeight = doc.createElement("InternalFragmentWeight")
            InternalFragmentWeight.appendChild(doc.createTextNode("1"))
            InternalFragmentAas = doc.createElement("InternalFragmentAas")
            InternalFragmentAas.appendChild(doc.createTextNode("KRH"))
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

        fp = open(output_path, "w", encoding="utf-8")
        doc.writexml(fp, indent="", addindent="\t", newl="\n", encoding="utf-8")
        fp.close()
        if len(self.warnings) != 0:
            for k, v in self.warnings.items():
                print('WARNING: "' + k + '" occured ' + str(v) + " times.")
        print("SUCCESS Convert " + sdrf_file + " to Maxquant parameter file")

    # create maxquant experimental design file
    def maxquant_experiamental_design(self, sdrf_file, output):
        sdrf = pd.read_csv(sdrf_file, sep="\t")
        sdrf = sdrf.astype(str)
        sdrf.columns = map(str.lower, sdrf.columns)
        f = open(output, "w")
        f.write("Name\tFraction\tExperiment\tPTM")
        for index, row in sdrf.iterrows():
            data_file = row["comment[data file]"][:-4]
            source_name = row["source name"]
            if "comment[fraction identifier]" in row:
                fraction = str(row["comment[fraction identifier]"])
                if "not available" in fraction:
                    fraction = ""
            else:
                fraction = ""
            if "comment[technical replicate]" in row:
                technical_replicate = str(row["comment[technical replicate]"])
                if "not available" in technical_replicate:
                    tr = "1"
                else:
                    tr = technical_replicate
            else:
                tr = "1"
            experiment = source_name + "_Tr_" + tr
            f.write("\n" + data_file + "\t" + fraction + "\t" + experiment + "\t")
        f.close()
        print("SUCCESS Generate maxquant experimental design file")

    def convert_path(self, path: str):
        seps = r"\/"
        sep_other = seps.replace(os.sep, "")
        return path.replace(sep_other, os.sep) if sep_other in path else path
