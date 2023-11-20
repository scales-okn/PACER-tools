'''
File: classify_parties.py
Author: Scott Daniel
Description: Classifies the names of parties in Pacer cases by labeling their component words
'''

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from support import data_tools as dtools
from support import lexicon

import re
import copy
import json
import spacy
import pandas as pd
from collections import Counter


#############
# Constants #
#############

lexicon_listnames_to_buckets = { # "listnames" are fine categories in lexicon.py, "buckets" are coarse but still finer than indiv-gov-other
    'company_acronyms_common': 'co',
    'company_acronyms_uncommon': 'co',
    'company_words_common': 'co',
    'company_words_uncommon': 'co',
    'english_words': 'eng',
    'english_words_supplementary': 'eng',
    'family_words': 'role',
    'family_words_plural': 'group',
    'given_names': 'person',
    'gov_words': 'gov',
    'gov_entities': 'gov',
    'group_words': 'group',
    'non_party_other': 'other',
    'non_party_people': 'role',
    'object_words': 'other',
    'org_words': 'org',
    'person_words': 'person',
    'professional_words': 'role',
    'role_words_non_professional': 'role',
    'surnames_common': 'person',
    'surnames_uncommon': 'person',
    'tlds_countries': 'co',
    'tlds_non_countries': 'co',
    'unknown_words': 'unknown',
    'unnamed_individual_words': 'person',
    'us_cities_deceptive_names': 'geo',
    'us_gov_entities_deceptive_names': 'gov',
    'us_state_abbrevs': 'geo',
    'us_state_names': 'geo'
}
lexicon_priority_order = [ # if a word gets multiple category labels, the one w/ the smaller index (i.e. higher in the list) takes precedence
    'professional_words', 'role_words_non_professional', 'group_words', 'person_words',
    'family_words', 'family_words_plural', 'unnamed_individual_words',
    'org_words', 'gov_words', 'gov_entities', 'us_gov_entities_deceptive_names', 'us_state_names', 'us_cities_deceptive_names',
    'unknown_words', 'non_party_people', 'non_party_other', 'object_words',
    'tlds_countries', 'tlds_non_countries',
    'company_words_common', 'company_words_uncommon', 'company_acronyms_common',
    'given_names', 'surnames_common',
    'english_words_supplementary', 'english_words', 'us_state_abbrevs',
    'surnames_uncommon',
    'company_acronyms_uncommon']
lexicon_lists_with_single_words_only = ['company_words_uncommon', 'english_words', 'given_names', 'surnames_common', 'surnames_uncommon']

multi_entity_regexes_priority_order = [ # class_action_re first to avoid later conflicts, then nonmutative before mutative to avoid extra work
    'class_action_re', 'ex_rel_re', 'behalf_re', 'by_through_re', 'as_xy_re', 'capacity_re', 'care_of_re']
multi_entity_regexes_mutative = ['class_action_re', 'as_xy_re', 'capacity_re', 'care_of_re']
multi_entity_regexes_nonmutative = ['ex_rel_re', 'behalf_re', 'by_through_re']

# default_nlp = spacy.load("en_core_web_trf") # commented out due to high gpu usage
empty_party_placeholder = '***SCALES empty party name***'




###########
# Regexes #
###########

delim_start = "(?:[ \n\'\-\/\(]|^)"
delim_middle = "[ ,;\n\'\.\-\/\(\)]"
delim_end = "(?:[ ,;\n\'\.\-\/\)]|$)"

### class actions ###

similarly_re = '(?:similar|simular|similiar|simarily|simlarly|simililiarly|dimilarly)'
situated_prefix_re = f'(?:(?:(?:o|i)n )?behalf|(?:as )?(?:a )?representative|class|all other|(?:others? )?{similarly_re}|so-)'
situated_re = f'{situated_prefix_re}.*situt?ated(?: known and unknown)?'

main_rep_re = '(?:(?:as )?(?:a )?representative.{1,10})'
class_prefixes_re = f'(?:{main_rep_re}?(?:and|of|for) (?:as )?(?:a |the )?(?:[a-z]+ )?|pertaining |representing (?:a |the )?(?:[a-z]+ )?)'
class_neg_lookb_re = '(?<!and )(?<!and a )(?<!and the )(?<!and the putative )(?<!and as )(?<!and as a )'
member_rep_re = '(?:a |the )?class (?:member|representative)'
class_re = f'(?:{class_prefixes_re}class|(?:(?:o|i)n )?behalf[^,]*{class_neg_lookb_re}class|as {member_rep_re}|{member_rep_re}s)'

class_action_re = f'(?is){delim_start}(?:{situated_re}|{class_re}){delim_end}'


### as X of Y ###

as_xy_neg_lookb_re = '(?<!business)(?<!trading|spelled)(?<!known|named)(?<!described)(?<!sued|well)'
as_xy_neg_looka_re = f'(?!(?:of|to|yet|such|(?:well )?as|s?he|they|it|are|provided|assigned|identified|(?:the(?:ir)? )?true names?){delim_end})'

of_re = ''.join(['(?<!as)', '(?<!next|city|town|laws|part)', '(?<!state|board|court|house)', '(?<!behalf|office|county)',
    '(?<!village|located|variety)', '(?<!district)(?<!department)']) + '(?:of|for|in|with|at|to) '

as_xy_re = f'(?is){as_xy_neg_lookb_re}({delim_start}as {as_xy_neg_looka_re}(.+?){of_re}(.+?)(?:{delim_start}as |$))'


### capacity ###

and_both_in_re = '(?:(?:and(?:\/or)?|&) )?(?:both |all )?(?:in )?(?:both |all )?'
capac_pronouns_re = '(?:her(?:\/his)? |his(?:\/her)? |their |its |the )'
capac_adjectives_re = '(?:indivi?d?ual|personal|(?:un)?offici?al|professional|corporate|representative|fiduciary)'
capac_clause_re = f'(?:{and_both_in_re}{capac_pronouns_re}|{and_both_in_re}{capac_pronouns_re}?{capac_adjectives_re}(?: capacit(?:y|ies))?,? )'

capacity_re = f'(?is){delim_start}(?!(?:individual|official|representative),|the ){capac_clause_re}*capacit(?:y|ies){delim_end}'


### ex rel / for the benefit of ###

fbo_re = 'f\.?\/? ?b\.?\/? ?o\.?\/? ?'
ex_rel_base_re = f'(?:ex\.? ?rel\.?:?(?:atione)?|on the relation of|for the (?:use (?:of )?(?:and benefit )?|benefit (?:(?:of )?and use )?)of|{fbo_re})'
ex_rel_re = f'(?is){delim_start}{ex_rel_base_re}{delim_end}'


### on behalf of ###

obo_re = 'o\.?\/? ?b\.?\/? ?o\.?\/? ?'
behalf_base_re = f'(?:(?:(?:o|i)n )?(?:the )?behalf of|(?:o|i)n (?:the )?behalf|{obo_re})'
behalf_re = f'(?is){delim_start}{behalf_base_re}{delim_end}'


### by and through ###

by_through_pronouns_re = '(?:her(?:\/his)?|his(?:\/her)?|their|its)'
both_by_and_through_re = f'(?:by (?:and|&) through|through (?:and|&) by)(?: {by_through_pronouns_re})?'
just_by_re = f'by(?: {by_through_pronouns_re})?(?! (?:special appearance|individuals|asset acquisition|virtuf?e of|reason of))'
just_through_re = f'through(?: {by_through_pronouns_re})?(?! (?:power of attorney|the course of discovery|doe|\d))'
by_through_re = f'(?is)(?:{delim_start}{both_by_and_through_re}|(?:\n|^)(?:{just_by_re}|{just_through_re})){delim_end}'


### c/o ###

care_of_neg_looka_re = '(?:and|at|of|defendant|et al|officer|official|chief|sergeant|sgt|badge|star|investigator|guard|supervisor|night shift)'
care_of_re = f'(?is)(?:\n|^)c\/o\:? (?=[a-z].* )(?!{care_of_neg_looka_re} )'




################################################
# Custom code for private individual redaction #
################################################

def name_redaction_tree(party, party_orig, party_composite, extra_info, nos_subtype,
                        label_names, label_names_ei, label_names_non_ei, elective_nlp):
    
    def _classify_with_nlp(text, pdetails):
        doc = elective_nlp(text)
        if doc.ents and doc.ents[0].text==text and doc.ents[0].label_== 'PERSON':
            return ('redact', pdetails)
        else:
            return ('no_redact', pdetails)
    
    # rules for parties with at least one label
    if label_names:

        # co, role, gov/org/group, person
        if 'co' in label_names:
            return ('no_redact', '"co" label')
        if 'role' in label_names and not any((' '+x+' ' in party_composite for x in lexicon.nonperson_words_role_superseding)):
            return ('no_redact', '"role" label')
        if 'gov' in label_names or 'org' in label_names or 'group' in label_names:
            return ('no_redact', '"gov"/"org"/"group" label')
        if label_names_non_ei and all((x=='person' for x in label_names_non_ei)):
            return ('redact', 'all "person" labels in non-ei')

        # eng/geo, unknown
        if all((x in ('eng','geo') for x in label_names)):
            return ('no_redact', 'all "eng"/"geo" labels')
        if (any((x in label_names for x in ('unknown', 'other'))) or any((
            x in party_composite for x in ('$', '_')))) and 'true name' not in party_composite:
            return ('no_redact', '"unknown," "other," "$", "_"')

        # and/doe
        if (' and ' in party or '&' in party or ' doe ' in party_composite) and 'true name' not in party_composite:
            return ('no_redact', '"and," "doe"')

        # for all others (mostly person-eng conflicts), classify as individual if person labels are the plurality
        label_counts = Counter(label_names)
        mode_label = max(label_names, key=lambda label: label_counts[label])
        is_plurality = lambda label: mode_label==label and not any((x!=label and label_counts[x]==label_counts[label] for x in label_names))
        if is_plurality('person'):
            return ('redact', 'plurality of "person" labels')

        # fallback case
        else:
            if nos_subtype in ('social_security',) and party_orig.count(' ') in range(1,4): # non-census names
                return ('redact', 'no labels & 1 to 3 spaces')
            else:
                return _classify_with_nlp(party_orig, 'spacy (fallback case)')

    # rules for labelless parties
    else:
        if nos_subtype=='property_rights': # online shops
            return ('no_redact', 'no labels & property rights')
        elif nos_subtype in ('crim','social_security','immigration') and ' '.join(party_composite.strip().replace(
            '\n',' ').split()).count(' ') in range(1,4) and re.search('[A-Za-z]', party_composite): # non-census names
            return ('redact', 'no labels & 1 to 3 spaces')
        else:
            return _classify_with_nlp(party_composite, 'spacy (no labels)')
        

def name_redaction_multient_handler(party_composite):

    if '\n' in party_composite and ' ' in party_composite.split('\n',1)[1]:
        first_ei_word = party_composite.split('\n',1)[1].split()[0]
        if first_ei_word.lower() in ['representative', 'executor', 'administrator', 'trustee', 'conservator', 'fiduciary',
            'representatives', 'executors', 'administrators', 'trustees', 'conservators', 'fiduciaries',
            'agent', 'receiver', 'nominee', 'subrogee', 'relator', 'estate',
            'agents', 'receivers', 'nominees', 'subrogees', 'relators', 'estates']:
            return True, party_composite

    as_subrogee_of = set(re.findall(f"{delim_start}'a\.?\/? ?s\.?\/? ?o\.?\/? ?'{delim_end}", party_composite))
    if as_subrogee_of:
        return True, party_composite

    for regex_name in multi_entity_regexes_priority_order:
        results = set(re.findall(globals()[regex_name], party_composite))
        if results:

            if regex_name=='as_xy_re':
                for result in results:
                    if any(x in ' '+result[1].lower()+' ' for x in [' '+y+' ' for y in [
                        *lexicon.role_words_non_professional, *lexicon.family_words, *lexicon.family_words_plural]]):
                        return 'redact', party_composite
                return 'no_redact', party_composite

            elif regex_name in ('class_action_re', 'by_through_re', 'care_of_re', 'behalf_re'): # ambiguous - return to main classifier
                for group in results:
                    party_composite = party_composite.replace(group, ' ')
                return False, party_composite

            else: # capacity_re, ex_rel_re
                return 'no_redact', party_composite

    return False, party_composite




############################
# Smaller helper functions #
############################

def _words_in_text(text, lexicon, use_space_delim=True, use_fast_search=False):
    if use_space_delim and use_fast_search:
        return list(set(text.split()).intersection(lexicon)) # this optimization yields a 5x speedup :)
    else:
        return [word for word in lexicon if (f' {word} ' if use_space_delim else word) in text]


def _is_initials(party):
    return (bool(re.match('(?i)[a-z]\. ?(?:[a-z]\. ?)?[a-z]\.(?:$|\n)', party)) and re.sub(
        '[\. ]','',party).lower() not in [*lexicon.professional_words, *lexicon.role_words_non_professional,
        *lexicon.gov_words, *lexicon.gov_entities, 'usa']) or bool(
        re.match('(?i)[a-z]\. [a-z]\.[a-z]\. [a-z]\.(?:$|\n)', party))


def _clean_party(party, remove_punc=True):

    party = party.lower()
    party = ' '+party+' ' # pad the whole string with spaces, for use as word boundaries
    party = re.sub('\n', ' \n ', party) # do the same around newlines
    party = re.sub('-', ' ', party) # replace hyphens with spaces in order to detect hyphenated last names etc
    party = re.sub("o\'", 'o ', re.sub("d\'", 'd ', party)) # fix "o'neill", "d'amico", etc

    # remove punctuation
    if remove_punc:
        for punc in '.,;:()[]"':
            party = party.replace(punc,'')

    return party


def _create_labels_for_word(text, word, label, use_space_delim=True):

    # escape special characters before regex search
    chars_to_escape = '.$()+*?|^'
    num_chars_escaped = 0
    if any((x in word for x in chars_to_escape)):
        for x in chars_to_escape:
            old_len = len(word)
            word = word.replace(x,'\\'+x)
            num_chars_escaped += len(word) - old_len

    # perform regex search and create a label for each match
    labels = []
    matches = list(re.finditer((f'(?= {word} )' if use_space_delim else f'(?={word})'), text))
    for match in matches:
        start = match.span()[0]+1 if use_space_delim else match.span()[0]
        span = (start, start+len(word)-num_chars_escaped)
        labels.append({ 'label':label, 'span':{'start':span[0],'end':span[1]} })

    return labels


def label_from_lexicon(party, listname_mappings):

    # handle initials
    if _is_initials(party):
        return ([{ 'label':'person_words', 'span':{'start':0,'end':len(party)} }], party)

    # handle website names (because we don't want to remove punctuation if the party is e.g. 'Amazon.com')
    has_tld = any((x in party for x in lexicon.tlds_countries+lexicon.tlds_non_countries))

    # prep variables
    party = _clean_party(party, remove_punc = not has_tld)
    lex_labels = []

    # iterate through word lists
    for list_name in listname_mappings.keys():
        use_space_delim = 'tlds' not in list_name
        use_fast_search = list_name in lexicon_lists_with_single_words_only

        # label any words that match the lexicon
        for word in _words_in_text(party, getattr(lexicon, list_name), use_space_delim=use_space_delim, use_fast_search=use_fast_search):
            lex_labels += _create_labels_for_word(party, word, list_name, use_space_delim=use_space_delim)

    return (lex_labels, party)




###########################
# Larger helper functions #
###########################

def _apply_second_order_labeling_procedures(lex_labels, party, listname_mappings):

    if '\n' in party:
        lex_labels_non_ei = [x for x in lex_labels if x['span']['start']<party.index('\n')]
        lex_labels_ei = [x for x in lex_labels if x not in lex_labels_non_ei]
    else:
        lex_labels_non_ei, lex_labels_ei = lex_labels, []
    _label_text = lambda label: party[label['span']['start']:label['span']['end']]

    # if the only english labels in the label set were assigned to certain professional-/role-adjacent words, change those labels
    if lex_labels_non_ei and all((label['label'] in ('person', 'eng') for label in lex_labels_non_ei)) and party.count(' ')>2: #no 1-word strings
        for labels_to_check, eng_words_to_check, replacement_label in ([ # switch fewer labels if only the non-ei labels are all 'person'/'eng'
            (lex_labels, lexicon.eng_words_professional_adjacent, listname_mappings['professional_words']),
            (lex_labels, lexicon.eng_words_role_non_professional_adjacent, listname_mappings['role_words_non_professional'])] if all((
            label['label'] in ('person', 'eng') for label in lex_labels_ei)) else [(lex_labels_non_ei, ['unknown', 'unnamed'], 'person')]):
            labeled_eng_words = [_label_text(label) for label in labels_to_check if label['label']=='eng']
            if all((x in eng_words_to_check for x in labeled_eng_words)):
                lex_labels = [{ 'label':replacement_label, 'span':label['span'] } if label in labels_to_check and label[
                'label']=='eng' else label for label in lex_labels]

    # similarly, if the extra_info is just "(not) individually," change those label(s)
    if '\n' in party and party.split('\n')[1].strip() in ('individually', 'not individually'):
        lex_labels = [{ 'label':listname_mappings['role_words_non_professional'], 'span':label['span'] } if label['span']['start']>party.index(
            '\n') else label for label in lex_labels]

    # edge case: if the string or extra_info begins with 'co'/'pa' (e.g. 'C.O. Smith,' 'co-personal reps'), then don't label as company word
    problematic_co_words = ('co', 'pa')
    if any((x in party for x in problematic_co_words)):
        for i,label in enumerate(lex_labels):
            start = label['span']['start']
            if (_label_text(label) in problematic_co_words) and (start==1 or ('\n' in party and start==party.index('\n')+2)):
                del lex_labels[i]
                break

    # 'municipal corporation' edge case
    if 'municipal corporation' in party:
        states_re = '|'.join([x+' ' for x in lexicon.us_state_names])
        municipal_corporation_re = f' an? (?:{states_re}|quasi |public agency and/or (?:quasi )?)?municipal (corporation) '
        match = re.search(municipal_corporation_re, party)
        if match:
            span_to_change = re.search(municipal_corporation_re, party).span(1)[0]
            lex_labels = [{ 'label':'government', 'span':label['span'] } if label['span']['start']==span_to_change else label for label in lex_labels]

    # 'virginia'/'washington' edge cases
    problematic_states = ('virginia', 'washington')
    if any((x in party for x in problematic_states)):
        lex_labels = [{ 'label':'person', 'span':label['span'] } if _label_text(label) in problematic_states else label for label in lex_labels]

    # 'USA'/'ICE' edge case
    if any((party == x or ('\n' in party and party.split('\n')[0]==x) for x in (' usa ', ' ice '))):
        if lex_labels:
            del lex_labels[0]
        lex_labels.append({ 'label':'gov', 'span':{'start':1,'end':4} })

    # 'MV' edge case
    if ' mv ' in party:
        for i,label in enumerate(lex_labels):
            if _label_text(label)=='mv' and label['span']['start'] != 1:
                del lex_labels[i]
                break

    # 'PO box' edge case
    if ' po box ' in party:
        for i,label in enumerate(lex_labels):
            if _label_text(label)=='po' and label['label']=='role' and any(((
                _label_text(x)=='box' and x['span']['start']==label['span']['start']+3) for x in lex_labels)):
                del lex_labels[i]
                break

    # 'Na' edge case
    if ' na ' in party and not (party.rindex(' na ')==len(party)-4 or party.split(' na ')[1][0]=='\n'):
        target_labels, needs_change = [], True
        for i,label in enumerate(lex_labels):
            if _label_text(label)=='na':
                target_labels.append(i)
            elif label['label']!='person':
                needs_change = False
        if needs_change:
            for i in target_labels:
                lex_labels[i]['label'] = 'person'

    # prisoner-number/a-number edge case
    for num_re in ('[^0-9]\d{5} \d{3}[^0-9]','(?i)([^0-9a-z])a ?#? ?[0-9\- ]{8,11}([^0-9])'):#latter via support.data_tools.remove_sensitive_info
        if re.search(num_re, party):
            for match in re.finditer(num_re, party): #redundant, but i'm too lazy to find out how to get the span from findall
                lex_labels.append({ 'label':'person', 'span':{'start':match.span()[0],'end':len(party)} })
            lex_labels = [x for x in lex_labels if x['label']=='person'] # maybe sensible but also maybe hacky and/or a misuse of _apply_second

    return lex_labels


def refine_labels(lex_labels, party, listname_mappings):

    if lex_labels:

        # set up variables needed for label refinement
        _span_tuple = lambda label: tuple(label['span'].values())
        _is_subset = lambda span1,span2: span1[0]>=span2[0] and span1[1]<=span2[1] # checks if span1 is a subset of span2
        _final_mapping = lambda label: listname_mappings[label['label']]
        inds_to_remove = set()

        # for each pair of labels...
        for i in range(0,len(lex_labels)-1):
            for j in range(i+1,len(lex_labels)):
                if i not in inds_to_remove and j not in inds_to_remove:

                # ...check if they occupy the same span...
                    if _span_tuple(lex_labels[i]) == _span_tuple(lex_labels[j]):

                        # if they differ in priority, the higher-priority label takes precedence (and if they're the same, we can remove either)
                        if lexicon_priority_order.index(lex_labels[i]['label']) > lexicon_priority_order.index(lex_labels[j]['label']):
                            lex_labels[i]['label'] = lex_labels[j]['label'] # j takes priority
                        inds_to_remove.add(j)

                    # ...and check if j is a subset of i, or vice versa (the assumption being that longer matches should supersede shorter ones)
                    elif _is_subset(_span_tuple(lex_labels[j]), _span_tuple(lex_labels[i])):
                        inds_to_remove.add(j)
                    elif _is_subset(_span_tuple(lex_labels[i]), _span_tuple(lex_labels[j])):
                        inds_to_remove.add(i)

        # map listnames to buckets, if necessary
            lex_labels[i]['label'] = _final_mapping(lex_labels[i])
        lex_labels[-1]['label'] = _final_mapping(lex_labels[-1])

        # remove the requisite labels
        for i in sorted(inds_to_remove, reverse=True):
            del lex_labels[i]

    # take a second pass through the labels and potentially modify them
    lex_labels = _apply_second_order_labeling_procedures(lex_labels, party, listname_mappings)

    return lex_labels or []


def _handle_multi_entities(party):

    ### begin temporary hard-coding ###
    # def _print_match(results, is_multi_match):
    #     party_for_display, results_for_display = party.replace('\n', ' '), [x.replace('\n', ' ').strip() for x in results]
    #     print(f"'{party_for_display}': {'matched' if is_multi_match else 'discarded'} '{results_for_display}'\n")

    multi_ent_role_words_rule_17 = [
        'representative', 'executor', 'administrator', 'trustee', 'guardian', 'conservator', 'fiduciary',
        'representatives', 'executors', 'administrators', 'trustees', 'guardians', 'conservators', 'fiduciaries']
    multi_ent_role_words_other = [
        'agent', 'receiver', 'nominee', 'subrogee', 'relator', 'heir', 'survivor', 'surviving',
        'beneficiary', 'successor', 'next of kin', 'next friend', 'next of friend', 'estate of',
        'agents', 'receivers', 'nominees', 'subrogees', 'relators', 'heirs', 'survivors',
        'beneficiaries', 'successors', 'next friends', 'estates of']
    multi_ent_role_words = multi_ent_role_words_rule_17 + multi_ent_role_words_other + lexicon.family_words + lexicon.family_words_plural
    
    if '\n' in party and ' ' in party.split('\n',1)[1]:
        first_ei_word = party.split('\n',1)[1].split()[0]
        if first_ei_word.lower() in multi_ent_role_words:
            # _print_match([first_ei_word], True)
            return True, party

    as_subrogee_of = set(re.findall(f"{delim_start}'a\.?\/? ?s\.?\/? ?o\.?\/? ?'{delim_end}", party))
    if as_subrogee_of:
        # _print_match([as_subrogee_of], True)
        return True, party
    ### end temporary hard-coding ###

    for regex_name in multi_entity_regexes_priority_order:
        regex_prelim = globals()[regex_name]
        regex = regex_prelim # regex = f'{regex_prelim}.+?(?={regex_prelim}|$)' (also add '(?:(?:and|&) )?'' as part of the connective tissue)
        # or, split on multiple separators to generate spans (& handle collisions, e.g. obo in class actions)

        results = set(re.findall(regex, party))
        if results:

            if regex_name in multi_entity_regexes_nonmutative:
                # _print_match(results, True)
                return True, party
            else:

                ### begin temporary hard-coding ###
                if regex_name == 'as_xy_re':
                    if any((x in groups[1].lower() for x in multi_ent_role_words for groups in results)):
                        # _print_match([x[1] for x in results], True)
                        return True, party
                    else:
                        # _print_match([x[0] for x in results], False)
                        for groups in results:
                            party = party.replace(groups[0], ' ')
                else:
                ### end temporary hard-coding ###

                    # _print_match(results, False)
                    for group in results:
                        party = party.replace(group, ' ')

    return False, party




#############
# Main flow #
#############

def classify_party(party, extra_info, nos_subtype, elective_nlp=None, include_labels_in_results=False,
    custom_classifier=None, custom_multient_handler=None, custom_listname_mappings=None):

    # helper function for choosing the correct return format
    def _get_return_values(pclass, pdetails):
        return (pclass, pdetails, lex_labels_unrefined, lex_labels, party, extra_info) if include_labels_in_results else (pclass, pdetails)

    # do some pre-screening (empty parties, dba words, multi-entity parties), and discard irrelevant text if there's only one entity of interest
    party_orig = party
    party_composite = (party or '') + '\n' + extra_info if extra_info else party
    lex_labels_unrefined, lex_labels = None, None
    if not party_composite:
        return _get_return_values('other', 'standard')
    if any((re.search(f'(?i)\\b{x}\\b'.replace('/','\/'), party_composite) for x in lexicon.dba_words)):
        return _get_return_values('other', 'standard')
    result, party_composite = custom_multient_handler(party_composite) if custom_multient_handler else _handle_multi_entities(party_composite)
    if result:
        return _get_return_values(('multi-entity' if type(result)==bool else result), 'standard') # handler can return bool or text (eg 'redact')

    # retrieve & refine the labels for any words that appear in the lexicon
    listname_mappings = lexicon_listnames_to_buckets.copy()
    if custom_listname_mappings:
        for k,v in custom_listname_mappings.items():
            listname_mappings[k] = v
    lex_labels_unrefined, party_composite = label_from_lexicon(party_composite, listname_mappings)
    lex_labels = refine_labels(copy.deepcopy(lex_labels_unrefined), party_composite, listname_mappings)

    # split up the labels list according to whether they appear in extra_info
    label_names = [x['label'] for x in lex_labels]
    if '\n' in party_composite:
        party, extra_info = party_composite.split('\n',1)
        line_break = party_composite.index('\n')
        label_names_ei = [x['label'] for x in lex_labels if x['span']['start']>=line_break]
        label_names_non_ei = [x['label'] for x in lex_labels if x['span']['start']<line_break]
    else:
        party, extra_info = party_composite, None
        label_names_ei = []
        label_names_non_ei = label_names
    

    # use a decision tree to determine the class
    if custom_classifier:
        return _get_return_values(*custom_classifier( # any custom function must take the below args & return a tuple (pclass, pdetails)
            party,
            party_orig,
            party_composite,
            extra_info,
            nos_subtype,
            label_names,
            label_names_ei,
            label_names_non_ei,
            elective_nlp
        ))
    else:

        # rules for parties with at least one label
        if label_names:
            
            # simplest rules
            if label_names_non_ei and all((x in ('person','role') for x in label_names_non_ei)):
                return _get_return_values('individual', 'standard')
            if 'co' in label_names_non_ei:
                return _get_return_values('other', 'standard')
            if 'co' in label_names_ei and 'role' not in label_names_ei:
                return _get_return_values('other', 'standard')

            # geo only, eng/geo, unknown
            if all((x=='geo' for x in label_names)):
                return _get_return_values('government', 'simple edge cases')
            if all((x in ('eng','geo') for x in label_names)):
                return _get_return_values('other', 'by eng/geo')
            if 'unknown' in label_names or 'other' in label_names or '$' in party_composite or '_' in party_composite:
                return _get_return_values('other', 'simple edge cases')

            # role, gov, org/group
            if 'role' in label_names and not any((' '+x+' ' in party_composite for x in lexicon.nonperson_words_role_superseding)):
                return _get_return_values('individual', 'by role')
            if 'gov' in label_names and 'org' not in label_names and 'group' not in label_names:
                return _get_return_values('government', 'standard')
            if 'org' in label_names or 'group' in label_names:
                return _get_return_values('other', 'by org/group')
            
            # least common rules
            if ' and ' in party or '&' in party:
                return _get_return_values('other', 'simple edge cases')
            if ' doe ' in party_composite:
                return _get_return_values('individual', 'simple edge cases')

            # for all others (mostly person-eng conflicts), classify as individual if person labels are the plurality
            label_counts = Counter(label_names)
            mode_label = max(label_names, key=lambda label: label_counts[label])
            is_plurality = lambda label: mode_label==label and not any((x!=label and label_counts[x]==label_counts[label] for x in label_names))
            if is_plurality('person'):
                return _get_return_values('individual', 'by plurality')

            # otherwise, bring in an external classifier
            else:
                # nlp = elective_nlp if elective_nlp else default_nlp # commented out due to high gpu usage
                if elective_nlp:
                    doc = elective_nlp(party_composite)
                    if doc.ents and doc.ents[0].text==party_composite and doc.ents[0].label_== 'PERSON': # ==party_composite.strip()?
                        return _get_return_values('individual', 'by spacy')
                    else:
                        return _get_return_values('other', 'by spacy')
                else:
                    return _get_return_values('other', 'fallback case (no external classifier available)') # added due to high gpu usage

        # rules for labelless parties
        else:
            num_words = party.count(' ')+1
            if num_words==1 and nos_subtype not in ('crim', 'prisoner_petitions', 'habeas_corpus', 'civil_detainee'):
                return _get_return_values('other', 'labelless')
            else:
                return _get_return_values('individual', 'labelless')