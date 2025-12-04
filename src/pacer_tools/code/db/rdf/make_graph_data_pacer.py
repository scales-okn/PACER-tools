"""
References to IFP data have been commented out for the PACER-tools version of this script,
our assumption being that anyone with the credentials needed to access our IFP data on Mongo
would simply download our already-generated graph files rather than running this script
(and that anyone hoping to convert new IFP data to RDF would use the --ifp-filepath option
rather than use our admittedly-convoluted legacy Mongo system). If we're incorrect about
this assumption, feel free to contact us at engineering@scales-okn.org.
"""

import ast
import json
import pickle
import math
import os
import re
import subprocess
import sys
import tempfile
import gc
import click
# import pymongo
import pandas as pd

from tqdm import tqdm
from glob import glob
from pathlib import Path
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from rdflib import Graph, Literal, URIRef, RDF, XSD

sys.path.append(str(Path.cwd().parents[1].resolve()))
import utils
from constants import SCALES, J, NC, FIPS
from support import data_tools as dtools
from support import entity_functions as efunc
from support import fjc_functions as fjc
from support import settings
# from support.mongo_connector import SCALESMongo


def write_graph_worker(graph, outdir, file_name=None):
    utils._write_graph_to_file(graph, outdir, file_name=file_name)

def _safe_index(lst, idx):
    if lst is None:
        return None
    return lst[idx]

def _make_ucid(court, case_id):
    case_id = re.sub("(?<=^\d)-", ":", case_id)
    case_id = re.sub("-[A-Z]+", "", case_id)
    return f"{court};;{case_id}"

def _make_blank_graph():
    g = Graph()
    g.bind("scales", SCALES)
    g.bind("j", J)
    g.bind("nc", NC)
    g.bind("fips", FIPS)
    g.bind("rdf", RDF)
    return g


def _load_ifp_data(ucids, db_instance):
    batch_size = len(ucids)
    while True:
        try:
            ifp_data = []
            for i in range(0, len(ucids), batch_size):
                ifp_data += list(
                    db_instance.ifp.find(
                        {"ucid": {"$in": ucids[i : min(i + batch_size, len(ucids))]}},
                        {"ucid": 1, "labels": 1},
                    )
                )
            return pd.DataFrame(ifp_data)
        except pymongo.errors.DocumentTooLarge:
            batch_size = int(batch_size / 2)
            pass


def _load_idb_data(ucids):
    subdf = dtools.load_unique_files_df().loc[ucids]
    all_idb_merges = None
    non_idb_columns = [
        "case_type",
        "court",
        "year",
        "fpath",
        "filing_date",
        "terminating_date",
        "case_id",
        "nature_suit",
        "judge",
        "recap",
        "is_multi",
        "is_mdl",
        "mdl_code",
        "has_html",
        "ucid_weak",
        "ucid_x",
        "ucid_y",
        "source",
        "source_x",
    ]

    dfs = []
    for year in set(subdf.year):
        case_types = (
            x for x in set(subdf[subdf.year.eq(year)].case_type) if x in ("cr", "cv")
        )
        for case_type in case_types:
            datafile = settings.FJC / f"idb/{case_type}{str(year)[-2:]}.csv"

            new_idb_merge = fjc.idb_merge(
                datafile,
                case_type,
                dframe=subdf[subdf.year.eq(year) & subdf.case_type.eq(case_type)],
                cols="all",
            )
            dfs.append(new_idb_merge[0])

    all_idb_merges = pd.concat(dfs, ignore_index=True)
    return (
        all_idb_merges.drop(non_idb_columns, axis=1, errors="ignore")
        .rename(columns={"source_y": "source"}, errors="ignore")
        .dropna(subset=["ucid"])
        .set_index("ucid")
    )


def _make_entity_lists(jdata):

    parties, judges, counsels, firms = jdata["parties"], [], [], []
    if jdata["case_type"] != 'cr':
        if jdata["judge"]:
            judges.append((jdata["judge"], "Assigned Judge", None))
        judges += [
            (judge_name, "Referred Judge", None)
            for judge_name in jdata["referred_judges"]
        ]

    for i, party in enumerate(parties):
        if jdata["case_type"] == "cr" and party["party_type"] == "defendant":
            judges.append((party["judge"], "Assigned Judge", i))
            judges += [
                (judge_name, "Referred Judge", i)
                for judge_name in party["referred_judges"]
            ]

        for j,counsel in enumerate(party["counsel"]):
            counsels.append((
                counsel["name"],
                counsel["designation"],
                counsel["entity_info"].get("address"),
                i,
            ))

            firm_name = counsel["entity_info"].get("office_name")
            if firm_name:
                firms.append((firm_name, j))

    return (parties, judges, counsels, firms)


def write_metadata_files(outdir, jel=None):

    def _parse_code(code):
        code = str(code)
        if len(code) < 5:
            code = "0" * (5 - len(code)) + code
        return code

    g, triples = _make_blank_graph(), []

    courtfile = pd.read_csv(settings.use_datastore(settings.COURTFILE)).set_index(
        "abbreviation"
    )
    for court in list(
        pd.read_csv(settings.use_datastore(settings.DISTRICT_COURTS_94), header=None)[0]
    ):
        court_uri = utils._make_generic_uri("Court", court)
        triples += [
            (court_uri, RDF.type, J.Court),
            (court_uri, J.CourtName, Literal(courtfile.at[court, "name"])),
            (
                court_uri,
                SCALES.isInCircuit,
                Literal(courtfile.at[court, "circuit"]),
            ),
            (court_uri, J.CourtCategoryCode, Literal("FED")),
        ]
        for code in [
            _parse_code(x) for x in ast.literal_eval(courtfile.at[court, "zip"])
        ]:
            triples.append((court_uri, NC.AddressPostalCode, Literal(code)))
        for code in [
            _parse_code(x) for x in ast.literal_eval(courtfile.at[court, "fips"])
        ]:
            triples.append((court_uri, FIPS.CountyCode, Literal(code)))

    for tr in triples:
        g.add(tr)
    if not Path(outdir).exists():
        os.makedirs(outdir)
    utils._write_graph_to_file(g, outdir, file_name="courts.ttl")

    if jel is not None:
        g, triples = _make_blank_graph(), []

        jel = jel.set_index("SJID")
        judgefile = pd.read_csv(settings.use_datastore(settings.JUDGEFILE)).set_index(
            "nid"
        )
        judgefile_nids = set(judgefile.index)

        for sjid in jel.index:
            judge_uri = utils._make_generic_uri('JudgeEntity', sjid)
            triples += [
                (judge_uri, RDF.type, J.Judge),
                (
                    judge_uri,
                    NC.PersonFullName,
                    Literal(jel.at[sjid, "Presentable_Name"]),
                ),
                (
                    judge_uri,
                    J.JudicialOfficialCategoryText,
                    Literal(jel.at[sjid, "SCALES_Judge_Label"]),
                ),
            ]
            for extra_key, property_name in [
                ("NID", "hasFJCJudgeDirID"),
                ("BA_MAG_ID", "hasUVAJudgeDirID"),
            ]:
                if not pd.isna(jel.at[sjid, extra_key]):
                    v = jel.at[sjid, extra_key]
                    triples.append(
                        (judge_uri, SCALES[property_name], Literal(v))
                    )

                    if extra_key == "NID":
                        triples.append((judge_uri, NC.URLID, URIRef(f'http://fjc.gov/node/{v}')))
                        nid = int(v)
                        if nid in judgefile_nids:
                            triples += [
                                (
                                    judge_uri,
                                    NC.PersonRaceText,
                                    Literal(
                                        judgefile.at[nid, "Race or Ethnicity"]
                                    ),
                                ),
                                (
                                    judge_uri,
                                    NC.PersonSexText,
                                    Literal(judgefile.at[nid, "Gender"]),
                                ),
                            ]
                            triples += [
                                (
                                    judge_uri,
                                    SCALES.hasCommissionDate,
                                    Literal(utils._date_to_xsd(x) if "/" in x else x,
                                        datatype=XSD.date),
                                )
                                for x in [
                                    judgefile.at[nid, f"Commission Date ({i})"]
                                    for i in range(1, 7)
                                ]
                                if not pd.isna(x)
                            ]
                            triples += [
                                (judge_uri, SCALES.appointedByParty, Literal(x))
                                for x in [
                                    judgefile.at[
                                        nid, f"Party of Appointing President ({i})"
                                    ]
                                    for i in range(1, 7)
                                ]
                                if not pd.isna(x)
                            ]

        for tr in triples:
            g.add(tr)
        utils._write_graph_to_file(g, outdir, file_name="judge_entities.ttl")


def add_case_json_to_graph(g, jdata, entity_lists):
    triples = []
    ucid = jdata["ucid"]

    is_cr = True if jdata["case_type"] == "cr" else False
    case_uri = utils._make_case_uri(ucid)
    # I added this a long time ago (by analogy to XML, to be a better NIEM user), and I don't think we actually need it
    # triples.append((rdflib.BNode(), NC.CourtCase, case_uri))
    triples.append((case_uri, RDF.type, NC.CourtCase))
    triples.append((case_uri, RDF.type, SCALES.CriminalCase if is_cr else SCALES.CivilCase)) # if "cv" in ucid else "OtherCase"))
    triples.append(
        (
            case_uri,
            NC.CaseGeneralCategoryText,
            Literal("criminal") if is_cr else Literal("civil"),
        )
    )

    court_uri = utils._make_generic_uri('Court', jdata['court'])
    triples.append((case_uri, J.CaseCourt, court_uri))

    if jdata["lead_case_id"]:
        lead_case_ucid = _make_ucid(jdata["court"], jdata["lead_case_id"])
        triples.append((utils._make_case_uri(lead_case_ucid), SCALES.hasMemberCase, case_uri))
    for related_case_id in jdata["related_cases"]:
        related_case_ucid = _make_ucid(jdata["court"], related_case_id)
        triples.append(
            (case_uri, SCALES.hasRelatedCase, utils._make_case_uri(related_case_ucid))
        )

    case_level_pairings = [
        ("case_id", NC.CaseDocketID),
        ("case_status", NC.StatusDescriptionText),
        ("filing_date", NC.StartDate),
        ("terminating_date", NC.EndDate),
        ("nature_suit", NC.CaseSubCategoryText),
        ("cause", J.StatuteKeywordText),
        ("jurisdiction", NC.JurisdictionText),
    ]
    for scales_field, rdf_property in case_level_pairings:
        if jdata[scales_field]:
            if "date" in scales_field:
                triples.append(
                    (
                        case_uri,
                        rdf_property,
                        Literal(utils._date_to_xsd(jdata[scales_field]),
                            datatype=XSD.date),
                    )
                )
            else:
                triples.append(
                    (case_uri, rdf_property, Literal(jdata[scales_field]))
                )

    party_dicts, judge_tuples, counsel_tuples, firm_tuples = entity_lists
    party_types = {}
    for i, party_dict in enumerate(party_dicts):
        party_uri = utils._make_party_uri(ucid, i)
        party_type = party_dict["party_type"]
        party_types[i] = party_type
        party_predicate = {
            "plaintiff": J.CaseInitiatingParty,
            "defendant": J.CaseDefendantParty,
        }.get(party_type) or SCALES.Party
        triples += [
            (party_uri, RDF.type, party_predicate),
            (case_uri, party_predicate, party_uri),
            (
                party_uri,
                NC.EntityName,
                Literal(party_dict["name"]),
            ),
        ]
        if party_dict["role"]:
            triples.append(
                (
                    party_uri,
                    J.ParticipantRoleCategoryText,
                    Literal(party_dict["role"]),
                )
            )
        extra_info = dtools.extra_info_cleaner(
            party_dict["entity_info"].get("raw_info")
        )
        if extra_info:
            triples.append(
                (
                    party_uri,
                    SCALES.hasExtraInfo,
                    Literal(extra_info),
                )
            )

        if is_cr:
            for charge, charge_type in [
                (x, "pending") for x in party_dict["pending_counts"]
            ] + [(x, "terminated") for x in party_dict["terminated_counts"]]:
                charge_uri = utils._make_charge_uri(jdata["ucid"], i, charge["pacer_id"])
                triples += [
                    (charge_uri, RDF.type, J.Charge),
                    (party_uri, J.PersonCharge, charge_uri),
                    (case_uri, J.CaseCharge, charge_uri),
                    (charge_uri, J.ChargeText, Literal(charge["text"])),
                    (charge_uri, SCALES.hasChargeType, Literal(charge_type)),
                    (
                        charge_uri,
                        J.ChargeSequenceID,
                        Literal(charge["pacer_id"]),
                    ),
                    (
                        charge_uri,
                        J.ChargeDisposition,
                        Literal(charge["disposition"]),
                    ),
                ]
            highest_opening, highest_terminated = (
                party_dict["highest_offense_level_opening"],
                party_dict["highest_offense_level_terminated"],
            )
            if highest_opening:
                triples.append(
                    (
                        party_uri,
                        SCALES.hasHighestOffenseLevelOpening,
                        Literal(highest_opening),
                    )
                )
            if highest_terminated:
                triples.append(
                    (
                        party_uri,
                        SCALES.hasHighestOffenseLevelTerminated,
                        Literal(highest_terminated),
                    )
                )

    for i, (judge_name, judge_role, assigned_to_dft_id) in enumerate(judge_tuples):
        judge_uri = utils._make_generic_uri("Judge", f"{ucid}_{i}")
        if assigned_to_dft_id:
            triples.append(
                (
                    judge_uri,
                    SCALES.assignedToDefendant,
                    utils._make_party_uri(ucid, assigned_to_dft_id),
                )
            )
        triples += [
            (case_uri, J.CaseJudge, judge_uri),
            (judge_uri, NC.PersonFullName, Literal(judge_name)),
            (
                judge_uri,
                J.CaseOfficialRoleText,
                Literal(judge_role),
            ),
        ]

    for i, (
        counsel_name,
        counsel_role,
        counsel_address, 
        originating_party_id,
    ) in enumerate(counsel_tuples):
        counsel_uri = utils._make_counsel_uri(ucid, i)
        counsel_predicate = {
            "plaintiff": J.CaseInitiatingAttorney,
            "defendant": J.CaseDefenseAttorney,
        }.get(party_types[originating_party_id]) or J.Attorney
        triples += [
            (counsel_uri, RDF.type, counsel_predicate),
            (
                counsel_uri,
                NC.PersonFullName,
                Literal(counsel_name),
            ),
            (
                utils._make_party_uri(ucid, originating_party_id),
                counsel_predicate,
                counsel_uri,
            ),
        ]
        if counsel_role:
            triples.append(
                (
                    counsel_uri,
                    J.CaseOfficialRoleText,
                    Literal(counsel_role),
                )
            )
        if counsel_address:
            triples.append(
                (
                    counsel_uri,
                    NC.ContactMailingAddress,
                    Literal(counsel_address),
                )
            )

    for i, (firm_name, originating_counsel_id) in enumerate(firm_tuples):
        firm_uri = utils._make_generic_uri('Firm', f'{ucid}_f{i}')
        triples += [
            (firm_uri, RDF.type, SCALES.Firm),
            (firm_uri, NC.OrganizationName, Literal(firm_name)),
            (
                utils._make_counsel_uri(ucid, originating_counsel_id),
                SCALES.Firm,
                firm_uri,
            ),
        ]

    table_uri = utils._make_generic_uri('DocketTable', jdata['ucid'])
    triples += [
        (table_uri, RDF.type, J.RegisterOfActions),
        (case_uri, J.RegisterOfActions, table_uri),
    ]
    entry_uris = []
    for i, entry in enumerate(jdata["docket"]):
        entry_uris.append(utils._make_docket_uri(jdata["ucid"], i))
        triples += [
            (entry_uris[i], RDF.type, J.RegisterAction),
            (table_uri, RDF[f"_{i}"], entry_uris[i]),
            (table_uri, J.RegisterAction, entry_uris[i]),
            (entry_uris[i], NC.AdministrativeID, Literal(entry["ind"])),
            (
                entry_uris[i],
                J.RegisterActionDate,
                Literal(utils._date_to_xsd(entry["date_filed"]),
                    datatype=XSD.date),
            ),
            (
                entry_uris[i],
                J.RegisterActionDescriptionText,
                Literal(entry["docket_text"]),
            ),
        ]
    for i, entry in enumerate(jdata["docket"]):
        for edge in entry["edges"]:
            triples.append(
                (entry_uris[i], SCALES.hasReferenceToOtherEntry, entry_uris[edge[1]])
            )

    for tr in triples:
        g.add(tr)
    return g


# def add_entity_data_to_graph(jdata, entity_lists, g):
#     ucid = jdata["ucid"]
#     triples = []

#     party_dicts, judge_tuples, counsel_tuples, firm_tuples = entity_lists
#     party_data = efunc.load_entity_data(ucid, "parties")
#     judge_data = efunc.load_entity_data(ucid, "judges")
#     counsel_data = efunc.load_entity_data(ucid, "counsels")
#     firm_data = efunc.load_entity_data(ucid, "firms")

#     if len(party_data):
#         main_parties, ei_parties = (
#             party_data[party_data.entity_source.eq("party")],
#             party_data[party_data.entity_source.eq("extra_info")],
#         )
#         main_parties, ei_parties = main_parties.reset_index(), ei_parties.reset_index()
#         for party_df, predicate in (
#             (main_parties, SCALES.isInstanceOfEntity),
#             (ei_parties, SCALES.hasPartyReferenceInExtraInfo),
#         ):
#             if len(party_df):
#                 party_blocks = list(party_df.party_block)
#                 for i, party_block in enumerate(party_blocks):
#                     agent_uri = _make_agent_uri(ucid, party_block)
#                     triples.append(
#                         (
#                             agent_uri,
#                             predicate,
#                             utils._make_generic_uri(
#                                 "PartyEntity", party_df.at[i, "SPID_Strong"]
#                             ),
#                         )
#                     )
#     if len(judge_data):
#         judge_data_header = judge_data[
#             judge_data.docket_source.eq(
#                 "parties" if jdata["case_type"] == "cr" else "header_metadata"
#             )
#         ]
#         judge_id_tracker = len(party_dicts)
#         most_recent_party_id = -1
#         referred_judge_counter = 0

#         for _, _, originating_party_id in judge_tuples:
#             judge_type = (
#                 "assigned_judge"
#                 if originating_party_id != most_recent_party_id
#                 else "referred_judges"
#             )
#             matches = judge_data_header[
#                 judge_data_header._entity_extraction_method.eq(judge_type)
#             ]
#             if originating_party_id is not None:
#                 matches = matches[matches.party_enum.eq(originating_party_id)]
#             if len(matches) > 1 and judge_type == "referred_judges":
#                 matches = matches[matches.judge_enum.eq(referred_judge_counter)]
#                 referred_judge_counter += 1

#             if len(matches) > 1:
#                 raise Exception("Found apparent judge-entity duplicates in case", ucid)
#             if len(matches) == 1:
#                 agent_uri = _make_agent_uri(ucid, judge_id_tracker)
#                 triples.append(
#                     (
#                         agent_uri,
#                         SCALES.isInstanceOfEntity,
#                         utils._make_generic_uri("JudgeEntity", list(matches.SJID)[0]),
#                     )
#                 )
#             most_recent_party_id = originating_party_id
#             judge_id_tracker += 1

#         docket_judge_data = judge_data[judge_data.docket_source.eq("line_entry")]
#         if len(docket_judge_data):
#             for i in docket_judge_data.index:
#                 docket_entry_uri = utils._make_docket_uri(
#                     ucid, int(docket_judge_data.at[i, "docket_index"])
#                 )
#                 triples.append(
#                     (
#                         docket_entry_uri,
#                         SCALES.hasJudgeReference,
#                         utils._make_generic_uri(
#                             "JudgeEntity", docket_judge_data.at[i, "SJID"]
#                         ),
#                     )
#                 )

#     if len(counsel_data):
#         counsel_id_tracker = len(party_dicts) + len(judge_tuples)
#         party_ids_seen = Counter()

#         for _, _, _, originating_party_id in counsel_tuples:
#             counsel_subnum = party_ids_seen.get(originating_party_id) or 0
#             matches = counsel_data[
#                 counsel_data.party_block.eq(originating_party_id)
#                 & counsel_data.counsel_subnum.eq(counsel_subnum)
#             ]

#             if len(matches) > 1:
#                 raise Exception(
#                     "Found apparent counsel-entity duplicates in case", ucid
#                 )
#             if len(matches) == 1:
#                 agent_uri = _make_agent_uri(ucid, counsel_id_tracker)
#                 triples.append(
#                     (
#                         agent_uri,
#                         SCALES.isInstanceOfEntity,
#                         utils._make_generic_uri(
#                             "CounselEntity", list(matches.SCID_Strong)[0]
#                         ),
#                     )
#                 )
#             party_ids_seen.update({originating_party_id: 1})
#             counsel_id_tracker += 1

#     if len(firm_data):
#         firm_id_tracker = len(party_dicts) + len(judge_tuples) + len(counsel_tuples)
#         party_ids_seen = Counter()

#         for _, _, originating_party_id in firm_tuples:
#             counsel_subnum = party_ids_seen.get(originating_party_id) or 0
#             matches = firm_data[
#                 firm_data.party_block.eq(originating_party_id)
#                 & firm_data.counsel_subnum.eq(counsel_subnum)
#             ]

#             if len(matches) > 1:
#                 raise Exception("Found apparent firm-entity duplicates in case", ucid)
#             if len(matches) == 1:
#                 agent_uri = _make_agent_uri(ucid, firm_id_tracker)
#                 triples.append(
#                     (
#                         agent_uri,
#                         SCALES.isInstanceOfEntity,
#                         utils._make_generic_uri("FirmEntity",
#                             list(matches.SFID_Strong)[0]),
#                     )
#                 )
#             party_ids_seen.update({originating_party_id: 1})
#             firm_id_tracker += 1

#     for tr in triples:
#         g.add(tr)
#     return g


def add_ontology_data_to_graph(onto, ucid, g):
    if type(onto) == float:
        return g
    triples = []
    case_uri = case_uri = utils._make_case_uri(ucid)
    predicate = SCALES.OntologyLabel

    seen = set()
    for i in onto.index:
        entry_uri = utils._make_docket_uri(ucid, onto.at[i, "row_ordinal"])
        label = onto.at[i, "label"]
        if label.startswith("attribute"):
            ontology_type = "OntologyLabel/CaseEvent/Attribute"
            label = label.replace("attribute_", "")
        else:
            ontology_type = "OntologyLabel/CaseEvent"
        label_uri = utils._make_generic_uri(ontology_type, label.replace(" ", "_"))

        triples.append((entry_uri, predicate, label_uri))
        if label not in seen:
            seen.add(label)
            triples.append((case_uri, predicate, label_uri))

    for tr in triples:
        g.add(tr)
    return g


def add_ifp_data_to_graph(ifp_row, ucid, g):
    if not list(ifp_row) or pd.isna(ifp_row["labels"]):
        return g

    triples = []
    for label in ifp_row["labels"]["docket"]:
        entry_uri = utils._make_docket_uri(ucid, label["row_ordinal"])
        triples.append(
            (
                entry_uri,
                SCALES.hasIfpLabel,
                utils._make_generic_uri("IfpLabel", label["label"]),
            )
        )
        if label["SJID"]:
            triples.append(
                (
                    entry_uri,
                    SCALES.hasIfpJudgeAttribution,
                    utils._make_generic_uri("JudgeEntity", label["SJID"]),
                )
            )

    for tr in triples:
        g.add(tr)
    return g


def add_idb_data_to_graph(idb_row, jdata, g):
    if not list(idb_row) or pd.isna(idb_row["filedate"]):
        return g

    case_uri = utils._make_case_uri(jdata["ucid"])
    triples = []

    for col, val in idb_row.items():
        if not pd.isna(idb_row[col]):
            triples.append(
                (
                    case_uri,
                    SCALES["hasIdb" + col.capitalize()],
                    Literal(idb_row[col]),
                )
            )

    for tr in triples:
        g.add(tr)
    return g


def process_case_worker(args):
    (
        ucid,
        indir,
        skip_cases,
        skip_annotations,
        onto_data,
        ifp_data,
        idb_data,
    ) = args
    if skip_cases and skip_annotations:
        return []

    try:
        if indir:
            court, year, stem = (
                ucid.split(";;")[0],
                ucid.split(":")[1].split("-")[0],
                ucid.split(";;")[1].replace(":", "-"),
            )
            with open(indir.rstrip("/") + f"/{court}/json/{year}/{stem}.json") as f:
                jdata = json.load(f)
        else:
            jdata = dtools.load_case(ucid=ucid)

        entity_lists = _make_entity_lists(jdata)

        g = _make_blank_graph()
        if not skip_cases:
            g = add_case_json_to_graph(g, jdata, entity_lists)
        if not skip_annotations:
            # g = add_entity_data_to_graph(g, jdata, entity_lists)
            if onto_data is not None:
                if isinstance(onto_data, list):
                    onto_df = pd.DataFrame(onto_data)
                    g = add_ontology_data_to_graph(onto_df, jdata["ucid"], g)
                elif isinstance(onto_data, pd.DataFrame):
                    g = add_ontology_data_to_graph(onto_data, jdata["ucid"], g)
                elif not (hasattr(onto_data, "__len__") and len(onto_data) == 0):
                    g = add_ontology_data_to_graph(onto_data, jdata["ucid"], g)
            if ifp_data is not None:
                if isinstance(ifp_data, dict):
                    ifp_series = pd.Series(ifp_data)
                    g = add_ifp_data_to_graph(ifp_series, jdata["ucid"], g)
                elif isinstance(ifp_data, pd.Series):
                    g = add_ifp_data_to_graph(ifp_data, jdata["ucid"], g)
            if idb_data is not None:
                if isinstance(idb_data, dict):
                    idb_series = pd.Series(idb_data)
                    g = add_idb_data_to_graph(idb_series, jdata, g)
                elif isinstance(idb_data, pd.Series):
                    g = add_idb_data_to_graph(idb_data, jdata, g)

        triples = list(g)
        if len(triples) == 0:
            print(f"Warning: No triples generated for case {ucid}")
        return triples

    except Exception as e:
        raise e
        # print(f"Error processing case {ucid}: {e}")
        # return []


@click.command()
@click.argument("outdir")
@click.option(
    "--indir",
    default=None,
    help="optionally, a non-standard directory from which to read case data",
)
@click.option(
    "--ucid-list",
    default=None,
    help='a csv file with a "ucid" column containing the ucids to be processed; if not passed, the unique files table will be used',
)
@click.option(
    "--skip-cases",
    default=False,
    is_flag=True,
    help="turns off the case-writing step",
)
@click.option(
    "--skip-metadata",
    default=False,
    is_flag=True,
    help="turns off the metadata-writing step",
)
@click.option(
    "--skip-annotations",
    default=False,
    is_flag=True,
    help="turns off the annotation-loading steps",
)
@click.option(
    "--ifp-filepath",
    default=None,
    help="optionally, a path to a flat file from which to load Mongo IFP data",
)
@click.option(
    "--max-records",
    default=2000,
    help="optionally, a number of records to process",
)
@click.option(
    "--max-workers",
    default=2,
    help="optionally, a number of workers to use for parallel processing",
)
def main(
    outdir,
    indir,
    ucid_list,
    skip_cases,
    skip_metadata,
    skip_annotations,
    ifp_filepath,
    max_records,
    max_workers,
):
    print("\nStarting...")

    outdir = Path(outdir)
    if not outdir.exists():
        outdir.mkdir(parents=True, exist_ok=True)

    if ucid_list:
        ucids = list(pd.read_csv(ucid_list)["ucid"])
    else:
        ucids = list(pd.read_csv(settings.DATA_EXPLORER_UCIDS)["ucid"]) # DATA_EXPLORER_UCIDS not included for PACER-tools, so ucid_list must be passed

    if not skip_metadata:
        print("\nLoading JEL (master judge-entity table)...")
        jel = efunc.load_JEL()
        print("\nWriting metadata files...")
        write_metadata_files(outdir, jel=jel)

    if not skip_annotations:
        # mc = SCALESMongo()
        # mc.connect()
        # mongo = mc.db
        print("\nLoading ontology data...")
        onto = pd.read_csv(
            settings.ONTOLOGY_LABELS, usecols=["ucid", "row_ordinal", "label"]
        )
        # print("\nLoading ifp data from Mongo...")
        # if ifp_filepath:
        #     ifp_data = pd.read_csv(ifp_filepath)
        #     ifp_data["labels"] = ifp_data["labels"].apply(lambda x: ast.literal_eval(x))
        # else:
        #     ifp_data = _load_ifp_data(ucids, mongo)
        print("\nLoading IDB data...")
        idb_data = _load_idb_data(ucids)
        if skip_metadata:
            print("\nLoading JEL (master judge-entity table)...")
            jel = efunc.load_JEL()

        onto = (
            pd.DataFrame(onto.groupby("ucid")).set_index(0).reindex(ucids).reset_index()
        )
        # if len(ifp_data):
        #     ifp_data = ifp_data.set_index("ucid").drop("_id", axis=1)
        # ifp_data = ifp_data.reindex(ucids)
        idb_data = idb_data.reindex(ucids)

        onto_list = list(onto.iloc[:, 1]) if isinstance(onto, pd.DataFrame) else onto
        # if isinstance(ifp_data, pd.DataFrame):
        #     ifp_list = ifp_data.to_dict(orient="records")
        # else:
        #     ifp_list = ifp_data
        if isinstance(idb_data, pd.DataFrame):
            idb_list = idb_data.to_dict(orient="records")
        else:
            idb_list = idb_data

    if skip_annotations and skip_cases:
        return
    print("\nProcessing ucids...")
    chunk_size = max_records
    overall_pbar = tqdm(total=len(ucids), desc="Processing cases")
    for start in range(0, len(ucids), chunk_size):
        end = min(start + chunk_size, len(ucids))

        if not skip_annotations:
            onto_chunk = onto.iloc[start:end]
            # ifp_chunk = ifp_data.iloc[start:end]
            idb_chunk = idb_data.iloc[start:end]

            onto_list = list(onto_chunk.iloc[:, 1])
            # ifp_list = ifp_chunk.to_dict(orient="records")
            idb_list = idb_chunk.to_dict(orient="records")

            process_args = [
                (
                    ucids[i],
                    indir,
                    skip_cases,
                    skip_annotations,
                    onto_list[i - start],
                    None, # ifp_list[i - start],
                    idb_list[i - start],
                )
                for i in range(start, end)
            ]

        else:
            process_args = [
                (
                    ucids[i],
                    indir,
                    skip_cases,
                    skip_annotations,
                    None, None, None,
                )
                for i in range(start, end)
            ]

        records_counter = 0
        global_graph = _make_blank_graph()

        with ProcessPoolExecutor(
            max_workers=max_workers,
        ) as process_executor, ThreadPoolExecutor(
            max_workers=max_workers
        ) as thread_executor:
            futures = {
                process_executor.submit(process_case_worker, args): args[0]
                for args in process_args
            }
            write_futures = []

            for future in as_completed(futures):
                triples = future.result()
                if triples:
                    for triple in triples:
                        global_graph.add(triple)
                    records_counter += 1

                    if records_counter >= max_records:
                        print(f"\nWriting batch of {records_counter} records...")
                        write_future = thread_executor.submit(
                            write_graph_worker, global_graph, outdir
                        )
                        write_futures.append(write_future)

                        records_counter = 0
                        global_graph = _make_blank_graph()

                overall_pbar.update(1)

        if records_counter > 0:
            print(f"\nWriting final batch of {records_counter} records...")
            utils._write_graph_to_file(global_graph, outdir)

        for future in as_completed(write_futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in write operation: {e}")

        global_graph = None
        onto_list = ifp_list = idb_list = None
        gc.collect()

    overall_pbar.close()


if __name__ == "__main__":
    main()
