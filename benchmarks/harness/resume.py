"""Which cells of a campaign already ran.

A campaign is dozens of hours long and is run in sessions, so it must be
restartable without redoing finished work. The unit is the cell, repetition
included.

Every CSV under results/<experiment>/ is scanned, not just today's: result_path
stamps the filename with the date, so a campaign resumed the next day would
otherwise find an empty file and start over.
"""

import os

import pandas as pd


def cell_key(values, key_columns):
    """A hashable identity for one cell, as strings.

    Values are stringified because a CSV round-trip returns "0" where the
    experiment passed 0. Key columns must therefore be categorical or integer -
    never a measured float.
    """
    return tuple(str(values.get(column, "")) for column in key_columns)


def completed_cells(out_dir, experiment, profile, key_columns):
    """Keys of every cell already written for this experiment and profile."""
    directory = os.path.join(out_dir, experiment)
    done = set()
    if not os.path.isdir(directory):
        return done
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".csv"):
            continue
        frame = pd.read_csv(os.path.join(directory, name),
                            dtype=str, keep_default_na=False)
        for _, row in frame.iterrows():
            if row.get("profile") != profile:
                continue
            done.add(cell_key(row, key_columns))
    return done
