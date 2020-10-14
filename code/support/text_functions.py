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

