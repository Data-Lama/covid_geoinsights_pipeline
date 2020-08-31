export PYTHONPATH=$PYTHONPATH:pipeline_scripts/functions/
export PYTHONPATH=$PYTHONPATH:pipeline_scripts/
python3 pipeline_scripts/encrypt_file.py BD20200818.xlsx ../data_repo/data/data_stages/bogota/raw/cases/cases_bogota.csv
