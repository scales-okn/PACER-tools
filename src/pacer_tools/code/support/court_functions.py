import re
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings

CARDINALS = ['northern', 'southern', 'eastern', 'western', 'central', 'middle']
STATEY2CODE = json.load(open(settings.STATEY2CODE))
COURTS_94 = [x.strip() for x in open(settings.DISTRICT_COURTS_94).readlines()]

re_state_codes = '|'.join(STATEY2CODE.values())
re_card = '|'.join(x[0].lower() for x in CARDINALS)
re_court_abbrev = rf"^(?P<state_code>{re_state_codes})(?P<ord_code>({re_card})?)d$"

# Read in court data
courtdf = pd.read_csv(settings.COURTFILE, index_col=0)

abbr2name_dict = dict(zip(courtdf.index, courtdf.courtname))
name2abbr_dict = dict(zip(courtdf.courtname, courtdf.index))

# Full name like "Oklahoma Western", useful for fjc
full_name = (courtdf.state +' ' +  courtdf.cardinal.fillna('')).str.strip()
fullname2abbr_dict = dict(zip(full_name,courtdf.index))

def make_courtname(row):
    '''
    Creates a court name that looks like [Cardinal]-[State]
    '''
    courtname = ''

    if type(row.cardinal) == str:
        courtname += row.cardinal + '-'

    courtname += row.state
    courtname = courtname.lower().replace(' ','-')
    return courtname

def abbr2name(abbr):
    '''
    Convert court abbreviate to court name
    inputs:
        abbr - 4-letter court abbreviation e.g. ilnd
    outputs:
        courtname: the name of the court e.g. north-illinois
    '''
    return abbr2name_dict[abbr]

def name2abbr(name, ordinal_first=True):
    '''
    Convert court abbreviate to court name
    inputs:
        name - court name e.g. 'northern illinois' or 'northern illinois'
    outputs:
        abbr - 4-letter court abbreviation e.g. ilnd
    '''
    if 'district' in name and 'columbia' not in name:
        name = name.replace('district', '').rstrip()

    # If the ordinal is not first, reverse it:
    if not ordinal_first:
        nlist = name.split()
        if nlist[-1] in CARDINALS:
            name = " ".join([nlist[-1], *nlist[:-1]])
    if ' ' in name or '-' not in name:
        name = name.lower().replace(' ', '-')

    return name2abbr_dict[name]

def abbr2full(abbr):
    '''
        Convert a court abbreviation to the full title format
        Ex. 'txsd' -> "U.S. District Court for the Southern District of Texas"

        Inputs:
            abbr (str) - court abbreviate
        Outputs:
            str
    '''
    #Get the court abbreviation cardinal direction and state name from the court dataframe
    try:
        cardinal = courtdf[courtdf.index.eq(abbr)].cardinal.values[0]
        cardinal = cardinal + ' ' if (type(cardinal)==str) else ''
        state = courtdf[courtdf.index.eq(abbr)].state.values[0]

        #Make the string
        return  f"U.S. District Court for the {cardinal}District of {state}"
    except IndexError:
        print("Error with court abbreviation:", abbr)
        return None

def classify(court_raw):
    ''' Classify any district court '''
    court = court_raw.lower()
    # Check if it already matches an abbreviation
    if re.match(re_court_abbrev, court):
        return court

    # Deal with DC separately as 'District' has problematic matching
    elif 'columbia' in court:
        return 'dcd'

    else:
        # Look for state and cardinal words
        court = re.sub('[-,]',' ', court).strip()
        state = re.search("|".join(STATEY2CODE.keys()), court)
        if not state:
            return
        elif state.group() == 'northern mariana islands':
            card_letter = ''
        else:
            cardinal = re.search("|".join(CARDINALS), court)
            if cardinal:
                card_letter = cardinal.group()[0] if cardinal else ''

            # Case with "District Court, N.D. Illinois"
            elif not cardinal and 'D.' in court_raw:
                #Search for cardinal letter (case sensitive)
                match = re.search("(?P<card_letter>[A-Z])\.", court_raw.replace("D.",''))
                card_letter = match.groupdict()['card_letter'].lower() if match else ''
            else:
                card_letter = ''


        # state code + cardinal letter + 'd' e.g. ilnd
        abbrev = f"{STATEY2CODE[state.group()]}{card_letter}d"
        return abbrev
