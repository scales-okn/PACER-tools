import os
import re
import ast
import json
import argparse
import sys
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from rdflib import Graph, Namespace, Literal, RDF, XSD

sys.path.append(str(Path.cwd().parents[1].resolve()))
import utils
from constants import SCALES, J, NC, NIBRS, FIPS
from support import settings

crosswalk_df = pd.read_csv(settings.NIBRS_CROSSWALK_CLAYTON).rename(
    lambda x: x.lower(), axis=1
)
crosswalk_dict = {
    x["ccccrimelabel"]: x["nibrsoffense"] for _, x in crosswalk_df.iterrows()
}
fulltexts_dict = {
    x["ccccrimelabel"]: x["cccdescrip"] for _, x in crosswalk_df.iterrows()
}
categories_dict = (
    pd.read_csv(settings.NIBRS_CATEGORIES_CLAYTON)
    .rename(lambda x: x.lower(), axis=1)
    .set_index("offense")
    .to_dict(orient="index")
) | {'DRUG/NARCOTIC OFFENSES, DRUG/NARCOTIC VIOLATIONS':
     {'aorb': 'A', 'crime category': 'DRUG/NARCOTIC OFFENSES', 'classification': 'SOCIETY',
      'nibrs_offense_code': '35A'}} # temp; remove when shelleen's new file is ready 
drugs_dict = {
    x["cccdescrip"]: x["nibrs_code"]
    for _, x in utils.parse_drugs(crosswalk_df, "cccdescrip", "clayton").iterrows()
}
_format_nibrs_charge = lambda x: x.replace(" ", "_").replace("/", "_").replace(",", "-")
_format_nibrs_code = lambda x: {
    "A": "GROUP A INCIDENT REPORT",
    "B": "GROUP B ARREST REPORT",
}[x]



def symlink_aware_rglob(base_dir, pattern):
    base = Path(base_dir)
    for root, dirs, files in os.walk(base, followlinks=True):
        for name in files:
            if Path(name).match(pattern):
                yield Path(root) / name


def _create_graph():
    g = Graph()
    g.bind("scales", SCALES)
    g.bind("j", J)
    g.bind("nc", NC)
    g.bind("nibrs", NIBRS)
    g.bind("fips", FIPS)
    g.bind("rdf", RDF)
    return g


def _make_metadata_graph():
    g = _create_graph()
    for court in ('ga-clayton-superior', 'ga-clayton-magistrate', 'ga-clayton-state',
        'ga-clayton-superior-civil', 'ga-clayton-magistrate-civil', 'ga-clayton-state-civil'):
        court_level = court.split('ga-clayton-')[1].split('-')[0]
        court_uri = utils._make_generic_uri('Court', court)
        g.add((court_uri, J.CourtName,
            Literal(f'{court_level.capitalize()} Court of Clayton County, Georgia')))
        g.add((court_uri, FIPS.CountyCode, Literal(13063)))
        g.add((court_uri, J.CourtCategoryCode, Literal({
            'superior':'SUP', 'magistrate':'MAG', 'state':'COC'}[court_level])))
        for zipcode in (30236, 30238, 30260, 30273, 30274, 30288, 30296, 30297):
            g.add((court_uri, NC.AddressPostalCode, Literal(zipcode)))
    return g


def process_entities(outdir):
    utils.process_entities(
        (settings.PARTY_DIS_CLAYTON, settings.PARTY_DIS_UNIVERSAL),
        outdir,
        utils._make_party_uri,
        ('ucid', 'party_ind'),
        filter_funcs={settings.PARTY_DIS_UNIVERSAL: (
            lambda df: df[df.court.eq('ga-clayton')])
        }
    )


def process_sentences(outdir):
    df = pd.read_csv(settings.SENTENCES_CLAYTON, low_memory=False)
    df = df[pd.isna(df.has_unknown_extra_info)]

    # Filter rows where sentence_types and sentence_months are not null
    df = df[df.sentence_types.notna() & df.sentence_months.notna()]

    ucids, docket_inds, count_inds, party_inds = (
        list(df.ucid),
        list(df.scales_ind),
        list(df.count_number),
        list(df.defendant_ind),
    )
    term_uris, charge_uris = set(), set()

    g = _create_graph()
    for i in tqdm(range(len(df)), desc="Processing sentences"):
        try:
            types = ast.literal_eval(df.iloc[i].sentence_types)
            lengths = ast.literal_eval(df.iloc[i].sentence_months)
        except (ValueError, SyntaxError) as e:
            print(f"Error parsing sentence data for row {i}: {e}")
            continue

        _normalize_int_from_df = lambda x: int(
            float(x)) if re.match(r'[0-9\.]+$', x) else x
        _make_synthetic_count_ind = lambda x: '_'.join((
            str(_normalize_int_from_df(x)),
            'from_annotation'))
        ucid, docket_ind, count_ind, party_ind = (
            ucids[i],
            docket_inds[i],
            count_inds[i],
            party_inds[i],
        )
        entry_uri = utils._make_docket_uri(ucid, docket_ind)
        party_uri = (
            utils._make_party_uri(ucid, party_ind) if not pd.isna(party_ind) else None
        )
        charge_uri = ( # != header charge uri; compare j:ChargeSequenceID for identity
            utils._make_charge_uri(ucid,
                (party_ind if not pd.isna(party_ind) else '-1'),
                _make_synthetic_count_ind(count_ind)) if not pd.isna(count_ind) else None
        )
        charge_uris.add(charge_uri)
        g.add((charge_uri, RDF.type, J.Charge))

        for j in range(len(types)):
            typ, length = types[j], lengths[j]
            sentence_uri = utils._make_sentence_uri(ucid, docket_ind, j)
            g.add((entry_uri, J.Sentence, sentence_uri))
            g.add((sentence_uri, RDF.type, J.Sentence))
            g.add(
                (
                    sentence_uri,
                    J.SentenceDescriptionText,
                    Literal(utils._escape_quotes(typ)),
                )
            )

            # Create term URI and add to set for later processing
            term_uri = utils._make_generic_uri("Term", str(length))
            term_uris.add(term_uri)
            g.add((sentence_uri, J.SentenceTerm, term_uri))

            if party_uri:
                g.add((party_uri, J.Sentence, sentence_uri))
            if charge_uri:
                g.add((charge_uri,
                       J.ChargeSequenceID,
                       Literal(_normalize_int_from_df(count_ind))))
                g.add((charge_uri, J.ChargeSentence, sentence_uri))

        if i and not i%50000:
            utils._write_graph_to_file(g, outdir, infix="sentences")
            g = _create_graph()

    for term_uri in term_uris:
        term_id = term_uri.split("/")[-1]
        num_months, num_days = term_id, None
        if "." in num_months:
            num_months, days_part = num_months.split(".")
            num_days = str(int(float(f"0.{days_part}") * 30))

        if num_months == "None":
            num_months = 0

        g.add((term_uri, RDF.type, J.SentenceTerm))
        g.add(
            (
                term_uri,
                J.TermDuration,
                Literal(
                    f"P{num_months}M{num_days}D" if num_days else f"P{num_months}M",
                    datatype=XSD.duration,
                ),
            )
        )  # XSD is wild for this...

    utils._write_graph_to_file(g, outdir, infix="sentences")
    return charge_uris # so as not to duplicate these uris' rdf.type triples


def process_json_file(json_file, charge_uris):
    try:
        with open(json_file, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {json_file}: {e}")
        return None

    g = _create_graph()

    # Top-level fields
    ucid = data.get("ucid")
    case_uri = utils._make_case_uri(ucid)
    g.add((case_uri, RDF.type, NC.CourtCase))
    g.add(
        (
            case_uri,
            RDF.type,
            SCALES.CivilCase if "civil" in ucid else SCALES.CriminalCase,
        )
    )
    g.add(
        (case_uri, NC.CaseDocketID, Literal(utils._escape_quotes(data.get("case_id"))))
    )
    g.add(
        (
            case_uri,
            NC.StatusDescriptionText,
            Literal(utils._escape_quotes(data.get("case_status"))),
        )
    )
    g.add(
        (
            case_uri,
            NC.JurisdictionText,
            Literal(utils._escape_quotes(data.get("jurisdiction", ""))),
        )
    )
    g.add((case_uri, J.CaseCourt, utils._make_generic_uri("Court", data.get("court"))))
    g.add(
        (
            case_uri,
            NC.CaseGeneralCategoryText,
            Literal("civil" if "civil" in ucid else "criminal"),
        )
    )
    for field, predicate in (
        ("filing_date", NC.StartDate),
        ("terminating_date", NC.EndDate),
    ):
        value = data.get(field)
        if value:
            g.add(
                (
                    case_uri,
                    predicate,
                    Literal(utils._date_to_xsd(value), datatype=XSD.date),
                )
            )

    # Parties
    parties = data.get("parties", [])
    for idx, party in enumerate(parties):
        party_uri = utils._make_party_uri(ucid, idx)
        party_predicate = {
            "plaintiff": J.CaseInitiatingParty,
            "defendant": J.CaseDefendantParty,
        }.get(party["party_type"], SCALES.Party)
        g.add((case_uri, party_predicate, party_uri))
        g.add((party_uri, RDF.type, SCALES.Party))
        g.add(
            (
                party_uri,
                NC.PersonFullName,
                Literal(utils._escape_quotes(party.get("name"))),
            )
        )
        g.add(
            (
                party_uri,
                J.ParticipantRoleCategoryText,
                Literal(utils._escape_quotes(party.get("role"))),
            )
        )
        for suit_type in party.get('suit_types', []):
            g.add((case_uri, NC.CaseSubCategoryText, Literal(suit_type)))

        # Charges (if present)
        for subidx, charge in enumerate(party.get("pending_counts", [])):
            # different than sentence charge uri; compare j:ChargeSequenceID for identity
            charge_uri = utils._make_charge_uri(ucid, idx, subidx)
            g.add((party_uri, J.PersonCharge, charge_uri))
            g.add((charge_uri, J.ChargeSequenceID, Literal(charge.get("datasource_id"))))
            g.add((charge_uri, J.ChargeText,
                Literal(utils._escape_quotes(charge.get("text")))))
            if charge_uri not in charge_uris:
                g.add((charge_uri, RDF.type, J.Charge))

            # NIBRS
            abbrev = charge["abbreviation"]
            nibrs_offense = crosswalk_dict[abbrev]
            if nibrs_offense in utils.manual_offense_mapping:
                g.add(
                    (
                        charge_uri,
                        NIBRS.OffenseUCRCode,
                        Literal(
                            _format_nibrs_charge(
                                utils.manual_offense_mapping[nibrs_offense]
                            )
                        ),
                    )
                )  # extra nibrs category, usually subcategory
            if (
                nibrs_offense not in categories_dict
            ):  # temp; remove when shelleen's new file is ready
                continue
            nibrs_category = categories_dict[nibrs_offense]["crime category"]
            if nibrs_category == "NOT A CRIME":
                continue
            g.add(
                (
                    charge_uri,
                    NIBRS.OffenseUCRCode,
                    Literal(_format_nibrs_charge(nibrs_category)),
                )
            )  # main nibrs category, as identified by shelleen
            for code in categories_dict[nibrs_offense]["aorb"].split(" OR "):
                g.add(
                    (
                        charge_uri,
                        NIBRS.NIBRSReportCategoryCode,
                        Literal(_format_nibrs_code(code)),
                    )
                )  # nibrs A/B dichotomy

            fulltexts = fulltexts_dict[abbrev]
            if fulltexts in drugs_dict:
                # print("Adding drug category for", charge_uri)
                g.add(
                    (charge_uri, J.DrugCategoryCode, Literal(drugs_dict[fulltexts]))
                )  # drug category

    # Judge
    judge_uri = utils._make_generic_uri("Judge", ucid)
    g.add((case_uri, J.CaseJudge, judge_uri))
    g.add((judge_uri, RDF.type, J.CaseJudge))
    g.add(
        (
            judge_uri,
            NC.PersonFullName,
            Literal(
                utils._escape_quotes(
                    data.get("judge")
                    or [x for x in data["parties"] if x["party_type"] == "defendant"][
                        0
                    ]["judge"]
                )
            ),
        )
    )

    # Docket
    docket = data.get("docket", [])
    if docket:
        table_uri = utils._make_generic_uri("DocketTable", ucid)
        g.add((case_uri, J.RegisterOfActions, table_uri))
        g.add((table_uri, RDF.type, J.RegisterOfActions))
    for idx, entry in enumerate(docket):
        entry_uri = utils._make_docket_uri(ucid, idx)
        g.add((table_uri, J.RegisterAction, entry_uri))
        g.add((entry_uri, RDF.type, J.RegisterAction))
        g.add(
            (
                entry_uri,
                J.RegisterActionDate,
                Literal(utils._date_to_xsd(entry.get("date_filed")), datatype=XSD.date),
            )
        )
        g.add(
            (
                entry_uri,
                J.RegisterActionDescriptionText,
                Literal(utils._escape_quotes(entry.get("docket_text"))),
            )
        )

    return list(g)


def write_graph_worker(graph, outdir, file_name=None, infix=None):
    """Worker function to write graph to file using thread executor."""
    utils._write_graph_to_file(graph, outdir, file_name=file_name, infix=infix)


def main(indir, outdir, skip_annotations):
    """
    Parse all JSON case files in indir and output a Turtle file every 50000 records.
    """
    indir = Path(indir)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Write a tiny metadata file
    utils._write_graph_to_file(_make_metadata_graph(), outdir, file_name='courts.ttl')

    # Write the annotation files
    charge_uris = set()
    if not skip_annotations:
        print('Clayton annotations not yet implemented in PACER-tools; skipping...')
        # print("Loading/processing sentences...")
        # charge_uris = process_sentences(outdir)
        # print("Loading/processing entities...")
        # process_entities(outdir)

    # Recursively find all JSON files in indir and subdirectories
    print(f"Loading JSON files...")
    json_files = [str(f) for f in symlink_aware_rglob(indir, "*.json") if f.is_file()]

    # Initialize counters and first graph
    records_counter = 0
    global_graph = _create_graph()

    with ProcessPoolExecutor(max_workers=12) as process_executor, ThreadPoolExecutor(
        max_workers=8
    ) as thread_executor:
        futures = {
            process_executor.submit(process_json_file, jf, charge_uris): jf for jf in json_files
        }
        write_futures = []

        with tqdm(total=len(json_files), desc="Processing cases") as pbar:
            for future in as_completed(futures):
                triples = future.result()
                if triples:
                    # Parse the N-Triples into the current global graph
                    for triple in triples:
                        global_graph.add(triple)
                    records_counter += 1

                    # If we've processed 10000 records, write to file and start a new graph
                    if records_counter >= 10000:
                        # Submit the write task to thread executor
                        write_future = thread_executor.submit(
                            write_graph_worker, global_graph, outdir
                        )
                        write_futures.append(write_future)

                        # Reset counters and create a new graph
                        records_counter = 0
                        global_graph = _create_graph()

                pbar.update(1)

    # Write any remaining records to a final file
    if records_counter > 0:
        utils._write_graph_to_file(global_graph, outdir)

    # Wait for all write operations to complete
    for future in as_completed(write_futures):
        try:
            future.result()
        except Exception as e:
            print(f"Error in write operation: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse all JSON case files in indir and output a Turtle file every 50000 records."
    )
    parser.add_argument("indir", help="Input directory containing JSON files")
    parser.add_argument("outdir", help="Output directory for Turtle files")
    parser.add_argument(
        "--skip-annotations",
        action="store_true",
        help="Process annotations",
    )
    args = parser.parse_args()
    main(args.indir, args.outdir, args.skip_annotations)
