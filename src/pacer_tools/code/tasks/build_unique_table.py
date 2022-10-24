import sys
from pathlib import Path

import click

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import data_tools as dtools

@click.command()
@click.option('--outfile', '-o', default=settings.UNIQUE_FILES_TABLE, show_default=True)
@click.option('--nrows', '-n', default=None)
def main(outfile, nrows):

    if outfile == settings.UNIQUE_FILES_TABLE:
        if not click.confirm(f"Overwrite the existing table at {outfile} ?"):
            return

    if nrows:
        nrows = int(nrows)

    df = dtools.generate_unique_filepaths(outfile, nrows)
    print(f"\nUnique filepaths table (with shape {df.shape}) output to {Path(outfile).resolve()}")

    exist_count = df.fpath.map(lambda x: (settings.PROJECT_ROOT/x).exists()).sum()
    print(f'\nFile existence check: {exist_count:,} / {len(df):,}')

if __name__ == '__main__':
    main()
