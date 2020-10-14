import seaborn as sns
from matplotlib.ticker import PercentFormatter
from matplotlib.ticker import FuncFormatter

# Palette
def pal(n=5, ind=False, cmap=False):
    '''
        Return a blue to orange palette

        Inputs:
            n (int) - no. of colours in the palette
            ind (int) - the index of the single colour in the palette to return
            cmap (bool) - whether to return a cmap
    '''
    h_neg, h_pos, s, l = 255, 22, 99, 65

    if cmap:
        return sns.diverging_palette(h_neg, h_pos, s, l, as_cmap=True)

    if n == 3:
        palette = [pal(4,ind)[i] for i in [0,2,3]]
    else:
        palette = sns.diverging_palette(h_neg, h_pos, s, l, n=n)

    #If index specified return a tuple of that color
    if type(ind)==int:
        return tuple(palette[ind])
    # Else return the whole palette
    else:
        return palette

# Graph label formatters
fmt_thou = FuncFormatter(lambda x,p: f"{x:,.0f}")
fmt_perc = PercentFormatter
