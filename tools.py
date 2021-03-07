import time
import matplotlib.pyplot as plt
import numpy as np


def benchmark(commands):
    """
    Function to run run_nb times a list of command and display stats and boxplot
    """
    run_nb = 10000
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

    plt.boxplot(run_times)
    plt.ylim(0, np.max(medians) * 2)
    plt.show()
