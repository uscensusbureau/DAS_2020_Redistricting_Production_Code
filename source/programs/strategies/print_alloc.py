from collections import defaultdict
import numpy as np
import pandas as pd
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from programs.strategies.strategies import StrategySelector
from constants import CC


LEVELS = CC.GEOLEVEL_BLOCK, CC.GEOLEVEL_BLOCK_GROUP, CC.GEOLEVEL_TRACT, CC.GEOLEVEL_COUNTY, CC.GEOLEVEL_STATE, CC.GEOLEVEL_US

print_functions = {
    'screen': str,
    'csv': lambda x: x.to_csv(),
    'latex': lambda x: x.to_latex()
}


def makeDataFrame(strategy_name, levels):
    strategy = StrategySelector.strategies[strategy_name].make(levels)
    d = defaultdict(dict)
    for level in strategy[CC.GEODICT_GEOLEVELS]:
        for q, qp in zip(strategy[CC.DPQUERIES][level], strategy[CC.QUERIESPROP][level]):
            d[level][q] = qp
    return pd.DataFrame(d).filter(reversed(levels))


def printFloat(df: pd.DataFrame, out='screen'):
    dff = df.astype(float)
    min_ord_mag = round(np.floor(-np.log10(dff.values[dff>0].min())))
    old_format = pd.options.display.float_format
    pd.options.display.float_format = '{:.1f}'.format
    pd.options.display.width = None
    s = print_functions[out]((dff.fillna(value=0) * 10**min_ord_mag).astype(float))
    pd.options.display.float_format = old_format
    return f"x 10^(-{min_ord_mag})\n{s}"


def printPercent(df: pd.DataFrame, out='screen'):
    dff = df.astype(float)
    old_format = pd.options.display.float_format
    pd.options.display.float_format = '{:.2f}%'.format
    s = print_functions[out]((dff.fillna(value=0) * (100 if out!='csv' else 1)).astype(float))
    pd.options.display.float_format = old_format
    return f"Percent:\n{s}" if out!='csv' else s


def multiplyByGLBudgets(df: pd.DataFrame, level_prop_it):
    for level, gl_prop in level_prop_it:
        df[level] = df[level] * gl_prop
    return df


def makeHeatTable(df: pd.DataFrame, fname:str):
    from matplotlib import pyplot
    #the_table = plt.table(cellText=df.values, rowLabels=df.index, colLabels=df.columns, loc='center')

    vals = df.fillna(value=0).values
    num_rows, num_cols = vals.shape

    normal = pyplot.Normalize(0,1)

    fig = pyplot.figure()

    ax = fig.add_subplot(111, frameon=False, xticks=[], yticks=[])

    ytable = pyplot.table(
        cellText=np.array(list(map(lambda d: f"{100*d:.2f}%", vals.ravel()))).reshape(vals.shape),
        rowLabels=df.index,
        colLabels=df.columns,
        colWidths=[.7/num_cols] * num_cols,
        loc='center',
        cellColours=pyplot.cm.Greens(normal(vals))
    )
    cellDict = ytable.get_celld()
    for i in range(0, num_cols):
        cellDict[(0, i)].set_height(.5/num_rows)
        for j in range(1, num_rows + 1):
            cellDict[(j, i)].set_height(.5/num_rows)
    ytable.set_fontsize(36)

    pyplot.savefig(fname, bbox_inches='tight',pad_inches = 0)

if __name__ == '__main__':
    df_main = makeDataFrame('ProductionCandidate20210517US', LEVELS)
    print(str(df_main))
    print(df_main.to_csv())
    print(df_main.to_latex())
    print(printFloat(df_main))
    print(printFloat(df_main, out='csv'))
    print(printFloat(df_main, out='latex'))
    print(printPercent(df_main))
    print(printPercent(df_main, out='csv'))
    print(printPercent(df_main, out='latex'))

    from fractions import Fraction as Fr
    dftotal = multiplyByGLBudgets(df_main, zip(reversed(LEVELS), (Fr(51, 1024), Fr(153, 1024), Fr(78, 1024), Fr(71, 1024), Fr(172, 1024), Fr(499, 1024))))
    print(str(dftotal))
    print(dftotal.to_csv())
    print(dftotal.to_latex())
    print(printFloat(dftotal))
    print(printFloat(dftotal, out='csv'))
    print(printFloat(dftotal, out='latex'))
    print(printPercent(dftotal))
    print(printPercent(dftotal, out='csv'))
    print(printPercent(dftotal, out='latex'))
    makeHeatTable(dftotal.astype(float), 'alloc.pdf')
