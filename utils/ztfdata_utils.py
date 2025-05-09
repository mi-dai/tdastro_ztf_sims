import pandas as pd
import glob
from nested_pandas import NestedFrame
import os

def parse_all_lightcurves(datadir,outdir='data/'):
    """
    Read all ZTF light curves.
    
    Parameters
    ----------
    datadir: str
        Path for the data directory
    outdir:
        Path for the output directory
    
    Returns
    -------
    lightcurves: nested_pandas.NestedFrame
        Table of all the light curves and their meta data
    
    """

    meta = pd.read_csv(os.path.join(datadir,'tables/snia_data.csv'))
    meta['SNID'] = meta['ztfname']
    meta = meta.drop(meta.columns[0],axis=1)
    
    df_list = []
    for f in glob.glob(os.path.join(datadir,'lightcurves/*.csv')):
        df = pd.read_csv(f, sep='\s+', comment='#')
        df['SNID'] = f.split('_lc')[0].split('/')[-1]
        df_list.append(df)
    lc = pd.concat(df_list)

    nf = NestedFrame(meta)
    nf = nf.add_nested(lc, on='SNID', name='lc')

    output_path = os.path.join(outdir,"ztfsniadr2.parquet")
    nf.to_parquet(
        output_path,  # The filename to save our NestedFrame to.
        by_layer=False,  # Save the entire NestedFrame to a single parquet file.
    )
    
    # List the files within our temp_path to ensure that we only saved a single parquet file.
    print("The NestedFrame was saved to the following parquet files :", os.listdir(outdir))
    
            
                    