import re
import string
import pandas as pd
from fuzzywuzzy import fuzz

regex_whitespace = '\s'
puncty_bound = '\b\s\.,:;\[\]\(\)&#'
accent_repl = str.maketrans("áàéêéíóöúñ","aaeeeiooun")
orgs = ['fbi', 'uscis', 'tsc', 'tx\. srv ctr', 'dcf', 'tx', 'dhs', 'd\.c\.f\.']
posts = ['director', 'secretary']
formal_suffixes = ['i', 'ii', 'iii', 'iv', 'v', 'jr', 'jnr', 'snr', 'sr', 'senior', 'junior']
human_affixes_abbrev = ['md', 'mr', 'mrs', 'ms', 'ofc', 'dr', 'ausa', 'esq', 'mph', 'dc', 'do', 'pt', 'dds', 'ps', 'ad']
human_affixes = ['archbishop', 'attorney', 'Attorney General', 'capt', 'captain', 'commissioner', 'corporal',
    'correction officer', 'deputy', 'detective', 'director', 'governor', 'Lieutenant', 'lt', 'manager', 'marshal', 'officer',
    'police officer', 'president', 'secretary', 'Sargeant', 'Sergeant', 'sgt', 'sheriff', 'trustee', 'U\.S\. Marshal', 'warden']




# def clean_for_fuzzy(text):
#     text = text.lower()
#     text = text.translate(str.maketrans('', '', string.punctuation))
#     return text


# def fuzzy_tagger(ents, entry_text, threshold=90):
    
#     fuzzy_tags = []
#     ents = sorted(ents, key=lambda tup: tup[1], reverse=True) # reverse sort by n_tokens
#     tokens, token_spans = zip(*[(m.group(), m.span()) for m in re.finditer(r'\S+', entry_text)])
    
#     restart_at = -1
#     for i in range(len(tokens)):
#         if i < restart_at:
#             continue
        
#         found_match = False
#         for ent_name, n_tokens in ents:
#             window = clean_for_fuzzy(' '.join(tokens[i:i+n_tokens]))
#             similarity_score = fuzz.ratio(window,ent_name)
#             if similarity_score > threshold:
                
#                 next_window_start = i+1 if (i+1)<len(tokens) else i
#                 next_window = clean_for_fuzzy(' '.join(tokens[next_window_start:next_window_start+n_tokens]))
#                 if similarity_score >= fuzz.ratio(next_window, ent_name):
                    
#                     # we are consuming this window
#                     fuzzy_start, fuzzy_end = i, i+n_tokens-1
#                     found_match = True
#                     break
        
#         # we found somebody on this window
#         if found_match:
#             fuzzy_tags.append((token_spans[fuzzy_start][0], token_spans[fuzzy_end][1]))
#             restart_at = i+n_tokens

#     return fuzzy_tags




def clean_name(name):
    name = name.strip()
    
    # omit prefixes (these patterns are lazy and do not substitute; they just cut the name at the prefix and advance forward)
    for pattern in [
        rf'(?i){"|".join([f"{p}({regex_whitespace}of|[,]+){regex_whitespace}{o}" for o in orgs for p in posts])}', # "director of"
        rf'(?i)(\b|\s)({"|".join(human_affixes)})(\b|\s)' # standard prefix
    ]:
        search = re.search(pattern, name)
        if search:
            name = name[:search.start()] if search.end()==len(name) else name[search.end()+1:] # account for "John Doe, Trustee"
    # (suffixes still need work)

    # order matters in execution (i.e. assuming numbers exist until numbers are stripped out)
    name = name.translate(accent_repl) # replace accented letters in case clerks did not use accents in entry
    
    name = re.sub(r'(?i)(((\\)?\'s|(?<=s)(\\)?\')(?= )|\\xc2|\\xa71)', r'', name) # replace as blanks
    name = re.sub(r'(?i)(\\xc3|\\xa1)', r'a', name) # replace as 'a'
    name = re.sub(r'\\\'', '\'', name) # make these normal apostrophes
    
    name = re.sub(r'[0-9]', r'', name) # no numbers please
    # name = re.sub(r'(!|"|#|%|&|\*|\+|,|/|=|\?|@|\^|_|`|~|\$|\||\\|<|>)', r' ', name) # dump meaningless punctuation
    # name = re.sub(r'({|\[|\(|\)|\]|})', r'', name) 
    name = re.sub(r'(!|"|#|%|&|\*|\+|,|/|=|\?|@|\^|_|`|~|\$|\||\\)', r' ', name) # dump meaningless punctuation
    name = re.sub(r'({|\[|\(|\)|\]|})', r'', name)

    name = re.sub(r'[.](?=[^\s])', r' ', name) # if a name is initial.initial, make that period a space
    name = re.sub(r'[.](?=[\s])', r'', name) # if a name has initial.space just strip the period
    name = re.sub(r'[\'](?=[\s])|(?<=[\s])[\']', r'', name) # if apostrophe space or space apostrophe, remove the space   
    name = re.sub(r'(-$|^-|\'$|^\')', '', name) # strip off end hyphens or apostrophes
    name = re.sub(r'(?<=[^\s]) [-](?=[^\s])|(?<=[^\s])[-] (?=[^\s])', '-', name) #collapse space surrounded hyphens
    
    # remove any remaining periods
    name = name.replace('.', '')
    
    # suffix normalization
    name = re.sub(r'(?i) (sr|snr|senior)', ' sr', name) # make all normal looking senior
    name = re.sub(r'(?i) (jr|jnr|junior)', ' jr', name) # make all normal looking jr

    # default split will remove odd double spacing, then rejoin
    return ' '.join(name.lower().split()).strip()


class anchor(object):
    ## custom class to identify word anchors and track their token neighborhoods
    ## Christian John Rozolis, Jr
    ## Anchor: Rozolis | Forward 1, tokens [Jr] | Backward [2] tokens [Christian, John] inits [C, J]
    # full string on input is assumed to be a human name of variant token length

    def __init__(self, instring):
        self.original_string = instring
        self.cleaned = clean_name(instring)
        self.name_lower = self.cleaned.lower()
        self.tokens = self.name_lower.split()

        # identify whether there are suffixes, and what the forward neighborhood should be
        enders = formal_suffixes + human_affixes_abbrev
        if self.tokens[-1] in enders and len(self.tokens)>1: #len check skips eg 'J.R.' (until classification can elim)
            self.has_suffix = True

            # double-suffixed, e.g. "MD, PhD"
            if self.tokens[-2] in enders and len(self.tokens)>2:
                self.anchor = self.tokens[-3]
                self.forward_neighborhood = 2
                self.forward_tokens = self.tokens[-2:]

            else:
                self.anchor = self.tokens[-2]
                self.forward_neighborhood = 1
                self.forward_tokens = [self.tokens[-1]]
        
        # no suffixes, anchor is the final token    
        else:
            self.has_suffix = False
            self.anchor = self.tokens[-1]
            self.forward_neighborhood = 0
            self.forward_tokens = []

        # how many tokens into the name string is it? (using index() assumes that the anchor only shows up once)
        self.backward_neighborhood = self.tokens.index(self.anchor)

        # identify backwards tokens
        self.backward_tokens = self.tokens[0:self.backward_neighborhood]
        self.backward_inits = [tok[0] for tok in self.backward_tokens]
        
    def __repr__(self): # custom printer
        pre = f"[{self.backward_neighborhood}] {self.backward_tokens} or {self.backward_inits}"
        post = f"[{self.forward_neighborhood}] {self.forward_tokens}"
        return f"ANCHOR: {self.anchor}\nPRE: {pre}\nPOST: {post}"
        



def qualified_anchor(anch, neighborhood):
    """ determines what tokens surrounding an anchor in an entry should be coded as part of the entity

    Args:
        anch (anchor(object)): custom class above
        neighborhood (dict): contains forward and backwards text/spans for the entry where the anchor was located

    Returns:
        new_start_span, new_end_span: both ints either None or the newly defined entry span for this entity text
    """

    new_start_span, new_end_span = None, None # by default, assume that only the anchor appeared (i.e. we only found a last name)

    if neighborhood['Backward']:
        neighborhood['Backward'].reverse()
        backward_spans = [(x[1],x[2]) for x in neighborhood['Backward']]
        neighborhood['Backward'] = list(map(lambda x: clean_name(x[0]), neighborhood['Backward']))

        # _bwd_break_cond_base: check if this token's first letter (x[0]) matches abbrev and next token (y[i+1]) is okay as well,
        # OR if this token is one we're looking for (latter clause permits "(john|j.) public" on top of "john q. public", which
        # is more permissive than chris's original code but still seems quite safe)
        _bwd_break_cond_base = lambda x,y,i: not x or (not ((x[0] in anch.backward_inits and y[
            i+1] in anch.backward_tokens+anch.backward_inits) or (x in anch.backward_tokens+anch.backward_inits)))
        _bwd_break_cond_last_token = lambda i: i!=len(neighborhood['Backward'])-1
        _bwd_break_cond_last_word = lambda j: j!=len(words)-1

        for i,token_text in enumerate(neighborhood['Backward']):
            words = [x.strip('.,:;') for x in token_text.split()]
            if any((
                any((
                    (not (word in anch.backward_tokens+anch.backward_inits or len(words)>1 or _bwd_break_cond_last_token(i))),
                    (len(words)==1 and _bwd_break_cond_last_token(i) and _bwd_break_cond_base(word, neighborhood['Backward'], i)),
                    (len(words)>1 and any(( # case where there are multiple words in token (could be 2 words, but usually just 1)
                        (not (_bwd_break_cond_last_word(j) or _bwd_break_cond_last_token(i))),
                        (_bwd_break_cond_last_word(j) and _bwd_break_cond_base(word, words, j)),
                        (_bwd_break_cond_last_token(i) and _bwd_break_cond_base(word, neighborhood['Backward'], i))
                    )))
                )) for j,word in enumerate(words))) or not words:
                if i>0:
                    new_start_span = backward_spans[i-1][0]
                break
            elif i==len(neighborhood['Backward'])-1: # we got to the end and didn't ever have to break, so include all the tokens
                new_start_span = backward_spans[i][0]

    if neighborhood['Forward']:
        _fwd_break_cond = lambda x: x not in anch.forward_tokens+anch.backward_tokens+anch.backward_inits

        for i,token in enumerate(neighborhood['Forward']):
            words = [x.strip('.,:;') for x in clean_name(token[0]).split()]
            if any((_fwd_break_cond(word) for word in words)) or not words:
                if i>0:
                    new_end_span = neighborhood['Forward'][i-1][2]
                break
            elif i==len(neighborhood['Forward'])-1: # we got to the end and didn't ever have to break, so include all the tokens
                new_end_span = neighborhood['Forward'][i][2]                  

    return new_start_span, new_end_span


def slice_neighborhood(tokens, is_back):
    opening_delimiters, closing_delimiters = ['<','(','[','{'], ['>',')',']','}']

    if is_back:
        tokens.reverse()
    filtered_tokens = []
    for token in tokens:
        filtered_tokens.append(token)
        if any((x in token[0] for x in (opening_delimiters if is_back else closing_delimiters))):
            break

    tokens, filtered_tokens = filtered_tokens.copy(), []
    for token in tokens:
        if any((x in token[0] for x in (closing_delimiters if is_back else opening_delimiters))):
            break
        filtered_tokens.append(token)
    if is_back:
        filtered_tokens.reverse()

    if not filtered_tokens:
        return []
    token_text, new_span_start, new_span_end = filtered_tokens[0 if is_back else -1] # if punct exists, it's in this token
    while any((x in token_text for x in (opening_delimiters if is_back else closing_delimiters))):
        for delim in (opening_delimiters if is_back else closing_delimiters):
            if delim in token_text:
                delim_index = token_text.index(delim)
                token_text = token_text[delim_index+1:] if is_back else token_text[:delim_index]
                new_span_start,new_span_end = (new_span_start+delim_index+1, new_span_end) if is_back else (
                    new_span_start, new_span_end-1)
    filtered_tokens[0 if is_back else -1] = (token_text, new_span_start, new_span_end)
    return filtered_tokens


def anchor_tagger(anchor, text, exact_matches):

    anchor_tags = []
    if len(anchor.anchor)<2: # these are v unlikely to be human names; more likely they're e.g. initials in already-redacted minors
        return anchor_tags

    # for every anchor we found in the entry, attempt the tagging process
    for anch in re.finditer(rf'(?i)(?<=[{puncty_bound}]){anchor.anchor}((\'s|(?<=s)\')(?= ))*[.,:,]*(?=[{puncty_bound}])', text):
        if any((anch.start()>=x[1] and anch.end()<=x[2] for x in exact_matches)): # shave off a bit of time by avoiding redundancy
            continue
        
        forward, backward = anchor.forward_neighborhood, anchor.backward_neighborhood # what neighborhood are we looking in?
        forward_max = forward + backward # default forward to the max
        pre, post = text[0:anch.start()], text[anch.end():] # identify the text before and after the anchor
        
        # split it into tokens, and slice out the desired tokens
        back = [(m.group(), *m.span()) for m in re.finditer(r'\S+', pre)][-backward:]
        forw = [(m.group(), anch.end()+m.start(), anch.end()+m.end()) for m in re.finditer(r'\S+', post)][0:forward_max]
        back, forw = slice_neighborhood(back, is_back=True), slice_neighborhood(forw, is_back=False)
        
        anchor_neighborhood = {'Backward':back, 'Forward':forw} # package into dictionary for anchor qualificaiton
        nss, nes = qualified_anchor(anchor, anchor_neighborhood) # determine if we need to adjust our spans for tagging

        anchor_tags.append((nss or anch.start(), nes or anch.end()))

    return anchor_tags




def tag_party_name_in_docket_texts(party_name, docket_texts, skip_anchor_tagger=False):

    tag_tuples = []
    party_anchor = anchor(party_name)
    for i, text in enumerate(docket_texts):
        if party_anchor.anchor not in text.lower(): # save time by pre-screening; will need to be removed if we restore fuzzy matching
            continue

        # exact matches
        exact_matches = []
        party_name_for_exact_matches=party_name.replace('(','\(').replace(')','\)').replace('*','\*').replace('?','\?')#nasty edgecases
        for m in re.finditer(rf'(?i)(?<=[{puncty_bound}]){party_name_for_exact_matches}(\'s)*[.,:,]*(?=[{puncty_bound}])', text):
            exact_matches.append((i, *m.span()))
        tag_tuples += exact_matches

        # inexact matches -- no fuzzy bc (1) such matches seem v rare & (2) refinement needed (e.g. don't just match on first-mid-last)
        if not skip_anchor_tagger:
            tag_tuples += [(i,*x) for x in anchor_tagger(party_anchor, text, exact_matches)]
        # tag_tuples += [(i,*x) for x in fuzzy_tagger([(clean_for_fuzzy(x), len(x.split())) for x in party_names], text)]

    # ending punctuation makes its way into the spans in various areas of the code; remove that punctuation before returning
    for i,tag in enumerate(tag_tuples):
        if docket_texts[tag[0]][tag[2]-1] in '.,:;':
            tag_tuples[i] = (tag[0], tag[1], tag[2]-1)

    return tag_tuples