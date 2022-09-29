import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import data_tools as dtools

def pro_se_identifier(party: dict, threshold: int=85):
    """Given a party dict from a SCALES json, identify if the party is PRO SE
    (if identified, returns index of counsel block that equals the party)

    Args:
        party (dict): SCALES json formatted party dict from a case
        threshold (int, optional): fuzzywuzzy fuzz.[MATCH_TYPE] ratio threshold to constitute a match. Defaults to 85.

    Returns:
        bool, int or NoneType: returns True if party is pro se, False if not; 
            second output:
            when bool is true - returns index of counsel block that is the pro se party
            when bool is false- returns NoneType
    
    Notes:
        This function requires a Bool flag to indicate success. When just an index or None was returned, explicit
        type checking was needed to confirm if a match occurred because None and 0 (a plausible counsel index return)
        both case to False -- the result was if type(output)==int: --> use int
    """
    
    ## -------------Internals---------------- ##
    
    def _return_success(index_selected):
        """format the function output with bool messaging

        Args:
            index_selected (int): the integer index of the counsel block that corresponds to the pro se party

        Returns:
            bool, int: returns True if party for party is pro se;  index of counsel block that is the pro se party
        """
        # during matching, the COUNSELS are given an attribute that tracks their original index in the counsels list
        # pop this if it was created so that the user's original input data remains unchanged upon return
        for counsel in COUNSELS:
            counsel.pop('original_index', None)
        return True, index_selected
    def _return_failure(rapid = False):
        """[summary]

        Args:
            rapid (bool, optional): if the function fails early before any data was changed, don't iterate through counsels. Defaults to False.

        Returns:
            bool, NoneType: party is not pro se; none since no index matches
        """

        if rapid:
            return False, None

        # during matching, the COUNSELS are given an attribute that tracks their original index in the counsels list
        # pop this if it was created so that the user's original input data remains unchanged upon return
        for counsel in COUNSELS:
            counsel.pop('original_index', None)
        return False, None
    
    def _call_fuzzy(party_name, counsels, match_type = 'ratio'):
        """call a fuzzy matching run across a list of counsels using the parent functions
        threshold, and the specified matching type

        Args:
            party_name (str): string of the party name being checked
            counsels (list): list of SCALES json formatted counsel dicts, with an added attribute for their original index
            match_type (str, optional): which fuzzywuzzy fuzz match should we employ. Defaults to 'ratio'.

        Returns:
            [type]: [description]
        """
        if match_type=='ratio':
            fuzzycall = fuzz.ratio
        elif match_type == 'token-set':
            fuzzycall = fuzz.token_set_ratio
        else:
            fuzzycall = fuzz.ratio
        
        if len(party_name) <=8:
            fuzzycall = fuzz.partial_token_set_ratio

        # failsafes are used to escape bad fuzzy matches before they happen
        # A. USA fuzzies into AUSA and many other generic X of USA roles. In general, we know the USA as a party
        # represents itself and that the term USA should not fuzzy into the individual counsel names
        # B. material witnesses and parties that are just abbreviations or single letters inadvertently match
        # their counsels middle initials or initialed names i.e. L.W. as a party matched James L. Watson.
        # if a party is a nondescript initial grouping, we do not fuzzy match it 
        FAILSAFES = [
            lambda party_name: party_name.lower().strip()=='usa',
            lambda party_name: all(len(tok.strip())==1 for tok in party_name.replace('.',' ').split()) 
        ]
        for failsafe in FAILSAFES:
            if failsafe(party_name):
                return None

        matches = [] # start with no matches
        for counsel in [c for c in counsels if c['name']]: # only compare counsels that had a name
            if len(counsel['name'])<=8:
                fuzzycall = fuzz.partial_token_set_ratio
            FR = fuzzycall( counsel['name'] , party_name ) # fuzzy match score
            if FR >= threshold: 
                matches.append((counsel, FR)) # if it matches, add to our matches
        if matches:
            # our threshold is high enough that any match is believable, if there are multiple, take the top one (?)
            # 0th index is top score
            # [0][0] is the counsel object in the tuple
            winner = sorted(matches, key = lambda tups: tups[1], reverse=True)[0][0]
            return _return_success(winner['original_index'])
        return None
    
    ## -------------------------------------- ##
       
    ## EARLY FAILSAFE
    # if any json keys are missing or NoneTypes, kick out
    if not party['counsel'] or not party['name']:
        return _return_failure(rapid=True)
    
    # will be using this everywhere
    COUNSELS = party['counsel']
    # add an attribute once that specifies the enumerated index of each iterable in the list
    # this saves us from continued enumeration and any ordering preservation
    for original_index, counsel in enumerate(COUNSELS):
        counsel['original_index'] = original_index
    
    ####################################
    # CONTROL BLOCK if restrictive pro se flag showed up in json from parse
    ####################################
    # if the parser already believes this to be a pro-se entry, leverage that as a head start
    if any((bool(counsel['is_pro_se']) for counsel in party['counsel'])):
        # if only one counsel, hooray no logic return it
        if len(COUNSELS)==1:
            return _return_success(0)

        # else: need to confirm that there is actually a "PRO SE" and there is only one counsel block that matches the criteria
        # looking for a singular "PRO SE" counsel
        matches = []
        for counsel in COUNSELS:
            if counsel['name']: # IF THERE IS A NAME FOR THE COUNSEL
                check = counsel['name']
                if counsel['entity_info'].get('raw_info'): # IF THERE IS ALSO RAW INFO
                    extra_info = dtools.extra_info_cleaner(counsel['entity_info'].get('raw_info'))
                    if extra_info:
                        check += '\n' + extra_info
            elif counsel['entity_info'].get('raw_info'): # THERE IS NO NAME, CHECK IF RAW INFO
                check = dtools.extra_info_cleaner(counsel['entity_info'].get('raw_info'))
            else: # NO NAME, NO RAW INFO.... THATS WHACK, WE CANT COMPARE IT
                continue
            
            # the explicit code that triggered the party level flag
            if "PRO SE" in check:
                matches.append(counsel)
        if len(matches)==1: # if only one counsel is pro se, return their original index
            return _return_success(matches[0]['original_index']) 


    ####################################
    # CONTROL BLOCK if party exactly represented text in counsels
    ####################################
    # dockets have whacky spacing on parties but not counsels sometimes -- normalize whitespace and case
    space_voider = lambda x: " ".join(x.strip().split()).lower()
    
    sv_party_name = space_voider( party['name'] ) # normalized party name
    sv_counsels = [(space_voider( c['name'] ), c['original_index']) for c in COUNSELS if c['name']] # normalized counsel names
    
    # if the normalized party appears in normalized counsel names verbatim, trigger and match
    if sv_party_name in sv_counsels:
        # (efficiency of "in" comparison presumed)
        # the match ends up as a tuple, return the original index for kick out
        match = [counsel for counsel in sv_counsels if counsel == sv_party_name][0]
        return _return_success(match[1])
    
    ####################################
    # CONTROL BLOCK TOKEN SET RATIO
    ####################################
    # final layer is a token set ratio check across the party and counsel names
    # fuzzywuzzy normalizes whitespace when generating tokens
    # if a party has prefixes, but the counsel form of the name does not, we still have
    # a successful token set match since one's tokens are wholly present in the others
    # the wrapper below will change match_type internally if a string is shorter than 9 characters
    from fuzzywuzzy import fuzz

    fuzzed = _call_fuzzy(party['name'], COUNSELS, match_type="token-set")
    if fuzzed:
        return fuzzed
    
    return _return_failure()


################################################
# Ngram similarity functions
################################################

def ngrams(string, n=3):
    import re
    string = re.sub(r'[,-./]|\sBD',r'', string)
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]

def cossim_top(A, B, ntop, lower_bound=0):
    import numpy as np
    import sparse_dot_topn.sparse_dot_topn as ct
    from scipy.sparse import csr_matrix
    # force A and B as a CSR matrix.
    # If they have already been CSR, there is no overhead
    A = A.tocsr()
    B = B.tocsr()
    M, _ = A.shape
    _, N = B.shape
 
    idx_dtype = np.int32
 
    nnz_max = M*ntop
 
    indptr = np.zeros(M+1, dtype=idx_dtype)
    indices = np.zeros(nnz_max, dtype=idx_dtype)
    data = np.zeros(nnz_max, dtype=A.dtype)

    ct.sparse_dot_topn(
        M, N, np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        ntop,
        lower_bound,
        indptr, indices, data)

    return csr_matrix((data,indices,indptr),shape=(M,N))

def get_matches_df(sparse_matrix):
    import pandas as pd
    non_zeros = sparse_matrix.nonzero()
    return pd.DataFrame({'left_side_idx': non_zeros[0], \
                         'right_side_idx': non_zeros[1], \
                         'similairity': sparse_matrix.data})

def swapper(tidx, name_vector):
    return name_vector[tidx]

###################################
# Basic cosine
###################################

