import os
import shutil

# Local imports
import functions.constants as con
import excecution_functions as ef
import special_reports.bogota.special_functions.utils as butils

# Global Directories
from global_config import config
data_dir = config.get_property('data_dir')
analysis_dir = config.get_property('analysis_dir')
report_dir = config.get_property('report_dir')

indent = '   '

# scripts
scripts = [ "ppr_gini.py",
            "extract_statistics.py",
            "superspreading_analysis.py",
            "super_spreader_shape.py",
            "attr_boxplots_localidades.py",
            "attr_boxplots.py",
            "housing_super_spreaders.py"]


export_location = {"ppr_gini.py":"graficas",
                    "extract_statistics.py":"archivos",
                    "superspreading_analysis.py": None, 
                    "super_spreader_shape.py": "shapefiles",
                    "attr_boxplots_localidades.py": "graficas",
                    "attr_boxplots.py": "graficas/observacion", 
                    "housing_super_spreaders.py": "shapefiles"}

# Scripts location for special report and export innit
butils.innit_export_file(scripts)
script_location = os.path.join("pipeline_scripts", "special_reports", "bogota")

# Excecutes
for script in scripts:
    print(ident + f"Excecuting {script}")
    resp = ef.excecute_script(script_location, script, "python", "", progress_file = con.progress_file)
    resp = "O.K" if resp == 0 else f"with error {resp}"
    print(f"{script} excecuted: {resp}")

# Exports 
export_folder_location = os.path.join(report_dir, "reporte_bogota")

if not os.path.exists(export_folder_location):
    os.makedirs(export_folder_location)   

print(ident + "Exporting files")
for script in scripts:
    files = butils.get_script_sources(script)

    if files == None:
        continue
    if export_location[script]:
        for file in files:
            file_name = file.split("/")[-1]
            print(f"{indent}{file_name}")
            dest_location = os.path.join(export_folder_location, export_location[script])

            # if shapefile copy entire folder
            if file_name.split(".")[-1] == "shp":
                shp_folder_path = "/".join(file.split("/")[:-1])
                shp_folder_name = file.split("/")[-2]
                dest_folder = os.path.join(dest_location, shp_folder_name)
                if not os.path.exists(dest_folder):
                    os.makedirs(dest_folder)

                butils.copytree(shp_folder_path, dest_folder)

            else:
                
                if not os.path.exists(dest_location):
                    os.makedirs(dest_location) 
                try:
                    shutil.copy(file, os.path.join(dest_location, file_name))

                except FileNotFoundError:
                    print(f'File Not Found: {file}')