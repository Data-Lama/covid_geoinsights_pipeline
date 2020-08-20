
# Excecution functions
import os
from datetime import datetime

import constants as con


def reset_progress(progress_file = con.progress_file):
    '''
    Resets progress
    '''

    with open(progress_file, 'w') as f:
        f.write('time,script_name,parameters,result\n')


def add_progress(script_name, parameters, result, progress_file = con.progress_file):
    '''
    Resets progress
    '''

    with open(progress_file, 'a') as f:
        f.write(f'{datetime.now()},{script_name},{parameters},{result}\n')


def excecute_script(script_location, name, code_type, parameters, progress_file = con.progress_file):
    '''
    Excecutes a certain type of script
    '''

    if code_type.upper() == 'PYTHON':

        #Python
        if not name.endswith('.py'):
            name = name + '.py'

        final_path = os.path.join(script_location, name)
        resp = os.system('{} {} {}'.format('python', final_path, parameters))

    elif code_type.upper() == 'R':
        #R
        if not name.endswith('.R'):
            name = name + '.R'

        final_path = os.path.join(script_location, name)
        resp = os.system('{} {} {}'.format('Rscript --vanilla', final_path, parameters))

    else:
        raise ValueError('No support for scripts in: {}'.format(code_type))


    add_progress(name, parameters, resp)

    return(resp)


