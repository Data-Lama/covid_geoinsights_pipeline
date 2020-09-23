export PYTHONPATH=$PYTHONPATH:pipeline_scripts/functions/
export PYTHONPATH=$PYTHONPATH:pipeline_scripts/
python pipeline_scripts/excecute_all.py

turn_off_instance=$1
if turn_off_instance
then
    sudo shutdown -h now
fi
