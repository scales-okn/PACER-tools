def bootstrap_district_variation(checkdf):
    '''
    For each courut does  a t test on the difference between an individual judge and the court and the other judges in the court
    Accounts for uneven sample sizes

    input:
    * checkdf - dataframe where each row is a case, columns are:
                ['court', 'judge', 'resolutuion']
                A positive outcome for the procedural ruling ('resolution') is 1 and a negative outcome is 0
                standard social science encoding
    output:
    * scidf - dataframe where each row is a judge, columns are:
              ['Judge', 'Diff', 'LB', 'UB',  'sig']
              diff is the actual difference, lb and ub are the confidence bounds, and sig is if 1 if it doesn't cross zero
    '''
    import numpy as np
    from scipy import stats
    import pandas as pd

    def _identify_sig(row):
        if np.sign(row['LB'])==np.sign(row['UB']):
            return 1
        else:
            return 0

    judge_data = []
    courts = [x for x in checkdf.court.unique() if x!='nmid']
    for court in courts:
        #Just subset to keep the naming shorter
        cdf = checkdf[checkdf.court == court]
        #Get the judge list
        judges = cdf.judge.unique()
        #District differences
        for j in judges:
            jdf = cdf[cdf.judge==j]
            njdf = cdf[cdf.judge!=j]
            mu_1 = np.mean(jdf.resolution)
            mu_2 = np.mean(njdf.resolution)
            s_1 = np.std(jdf.resolution)
            s_2 = np.std(njdf.resolution)
            diff = (mu_1-mu_2)
            #Uneven samples
            se = np.sqrt(s_1**2/len(jdf) + s_2**2/len(njdf))
            ndf = (se**2)**2/( (s_1**2/len(jdf))**2/(len(jdf)-1) + (s_2**2/len(njdf))**2/(len(njdf)-1) )
            lb = diff - stats.t.ppf(0.975, ndf)*se
            ub = diff + stats.t.ppf(0.975, ndf)*se
            
            judge_data.append([j, diff, lb, ub])

    scidf = pd.DataFrame(judge_data, columns = ['Judge', 'Diff', 'LB', 'UB'])
    scidf['sig'] = scidf.apply(_identify_sig, axis=1)
    return scidf
