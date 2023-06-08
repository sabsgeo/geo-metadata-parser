from multiprocessing import Process
import psutil
from time import sleep
import random

def add_data_in_parallel(funtion_to_call, list_to_parallel_other_params, number_of_process, min_mem_in_system, shuffle_list):
    list_to_parallel = list_to_parallel_other_params.get('list_to_parallel')
    if len(list_to_parallel) < number_of_process:
        number_of_process = int(len(list_to_parallel) / 2)
    
    if number_of_process < 1:
        number_of_process = 1

    if shuffle_list:
        random.shuffle(list_to_parallel)
    window_span = int(len(list_to_parallel) /  number_of_process)
    proc = []
    print("Process going to start")
    for par_pro_inx in range(number_of_process):
        sub_series_list = list_to_parallel[par_pro_inx *
                                   window_span: (par_pro_inx * window_span + window_span)]
        params = dict(list_to_parallel_other_params)
        params["list_to_parallel"] = sub_series_list
        p = Process(target=funtion_to_call, args=(params,))
        proc.append(p)
        print("Going to start " + p.name)

    for p in proc:
        p.start()
        print("Started " + p.name)
    
    while True:
        count = 0
        for p in proc:
            if not(p.exitcode == None):
                count = count + 1
        if psutil.virtual_memory().free >> 20 < min_mem_in_system:
            print(psutil.virtual_memory().free >> 30)
            print("Exiting because of low memory")
            break

        if count > number_of_process / 2:
            print("Number of process is too low")
            break

        sleep(3)
    
    for p in proc:
        if p.is_alive():
            p.terminate()

    for p in proc:
        p.join()
        print("Stopped " + p.name)