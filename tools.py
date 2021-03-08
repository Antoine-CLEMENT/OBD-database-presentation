import time
import matplotlib.pyplot as plt
import numpy as np

import logging
import threading
import redis


def benchmark(commands, run_nb=10000):
    """
    Function to run run_nb times a list of command and display stats and boxplot
    """
    run_times = []
    medians = []
    for command_idx, command_name in enumerate(commands):
        run_time = []
        for run in range(run_nb):
            start_time = time.time()
            commands[command_name]()
            end_time = time.time()
            run_time.append(end_time - start_time)

        median = np.median(np.array(run_time))
        std = np.std(np.array(run_time))
        mean = np.mean(np.array(run_time))
        print("---- " + str(command_idx) + ": " + command_name + " ----")
        print('average time:' + str(mean) + '\n median:' + str(median) + '\n std:' + str(std))
        run_times.append(run_time)
        medians.append(median)

    fig, ax1 = plt.subplots(figsize=(5, 5))
    ax1.boxplot(run_times)
    ax1.set_xticklabels(commands.keys(),
                        rotation=45, fontsize=8)
    ax1.set_ylim(0, np.max(medians) * 2)
    plt.show()


def thread_function(name, command, engine, run_nb, time_storage):
    """
       Function to run run_nb times a  command in one thread
       """
    logging.info("Thread %s: starting", name)
    # redis
    if engine is None:
        connexion = redis.Redis()
    # postgre
    else:
        connexion = engine.connect()

    time_list = []
    for i in range(run_nb):
        start_time = time.time()
        command(connexion)
        end_time = time.time()
        time_list.append(end_time - start_time)

    time_storage[name] = time_list

    logging.info("Thread %s: finishing", name)


def thread_run(command, client_nb, run_nb, engine):
    """
       Function call client_nb thread to run run_nb times the same command
       """
    threads = list()
    time_storage = {}

    for index in range(client_nb):
        logging.info("Main    : create and start thread %d.", index)
        x = threading.Thread(target=thread_function, args=(index, command, engine, run_nb, time_storage))
        threads.append(x)
        x.start()

    for index, thread in enumerate(threads):
        logging.info("Main    : before joining thread %d.", index)
        thread.join()
        logging.info("Main    : thread %d done", index)

    concat_time = []
    for thread_time in time_storage:
        concat_time += time_storage[thread_time]
    print("CONCAT TIME")
    print(len(concat_time))
    return concat_time


def benchmark_thread(commands, engine, run_nb, client_nb, show):
    """
         Main Function compute thread concurrency stats between several commands
         """
    run_times = []
    medians = []

    # iterate through commands and execute thread
    for command_idx, command_name in enumerate(commands):
        # redis
        if commands[command_name][1] == 'r':
            run_time = thread_run(commands[command_name][0], client_nb, run_nb, None)
        else:
            run_time = thread_run(commands[command_name][0], client_nb, run_nb, engine)

        median = np.median(np.array(run_time))
        std = np.std(np.array(run_time))
        mean = np.mean(np.array(run_time))
        print("---- " + str(command_idx) + ": " + command_name + " ----")
        print('average time:' + str(mean) + '\n median:' + str(median) + '\n std:' + str(std))
        run_times.append(run_time)
        medians.append(median)

    if show:
        fig, ax1 = plt.subplots(figsize=(5, 5))
        ax1.boxplot(run_times)
        ax1.set_xticklabels(commands.keys(),
                            rotation=45, fontsize=8)
        ax1.set_ylim(0, np.max(medians) * 2)
        plt.show()
    return medians
