from argparse import ArgumentParser
import pandas as pd
import os

def merge():
    p = ArgumentParser()
    p.add_argument('update', help='Comma separated list of columns to update in opd_source_table.csv from tmp.csv')
    p.add_argument('-d', '--diff', default=None, help='Comma separated list of columns that differ between opd_source_table.csv from tmp.csv. '+
                   'Defaults to update. This can be used if there are columns that differ but should not be updated.')
    p.add_argument('-m','--merge', action='store_true', help='Whether to merge and save. If not set, only diffs will be evalauted')
    args = p.parse_args()

    update_cols = [x.strip() for x in args.update.split(',')]

    changed_cols = [x.strip() for x in args.diff.split(',')] if args.diff else []
    changed_cols.extend(update_cols)

    base_csv = 'opd_source_table.csv'
    new_csv = 'tmp.csv'

    if not os.path.exists(base_csv):
        base_csv = os.path.join('..', base_csv)
        new_csv = os.path.join('..', new_csv)

    base = pd.read_csv(base_csv)
    new = pd.read_csv(new_csv)
    
    assert len(base)==len(new), 'Merge cannot currently be performed if number of rows has changed'

    for c in new.columns:
        assert c in base, 'Need to add handling of adding new columns'
        try:
            pd.testing.assert_series_equal(base[c], new[c])
            if c in changed_cols:
                print(f'WARNING: Column {c} was expected to differ but does not')
        except:
            if c not in changed_cols:
                print(f'WARNING: Column {c} was NOT expected to differ but does')
            else:
                print(f'Column {c} differs as expected')

    if args.merge:
        for c in update_cols:
            base[c] = new[c]

        base.to_csv(base_csv, index=False)


if __name__=='__main__':
    merge()