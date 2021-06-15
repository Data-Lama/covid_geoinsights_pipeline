import os
import shutil
import numpy as np
import pandas as pd


# local imports
import special_reports.bogota.bogota_constants as cons

def innit_export_file(scripts, export_file=cons.export_file):
    df_exports = pd.DataFrame(columns=["script", "source"])
    for s in scripts:
        df_exports = df_exports.append({'script': s, "source":""}, ignore_index=True)

    df_exports.to_csv(export_file, sep="\t", index=False)

def add_export_info(script_name, locations, export_file=cons.export_file):
    """
        Updates the export information
        parameters:
            - script_name (str): name of the script corresponding to export sources
            - locations (list): list of sources where files are saved
            - export_file:
    """

    df_exports = pd.read_csv(export_file, sep="\t")
    df_exports.loc[(df_exports["script"] == script_name), "source"] = ",".join(locations)

    df_exports.to_csv(export_file, sep="\t", index=False)

def get_script_sources(script_name, export_file=cons.export_file):
    df_exports = pd.read_csv(export_file, sep="\t")
    files = df_exports.set_index("script").at[script_name, "source"]
    if isinstance(files, float):
        return None
    else:
        return files.split(",")

def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)