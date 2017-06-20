""" Test Module """
from job import Job, JobState
from jobtree import JobTree, Machine
from scheduler import Scheduler
import os
import logging

results = ["NOT_TESTED", "SCHEDULED", "RUNNING", "OK", "FAILED", "SKIPPED"]
def show(alist):
    """ simple printer """
    for job in alist:
        print "%s --- > %s" % (job.name, results[job.status])
        show(job.children)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - (%(threadName)-15s) %(message)s')
    job_list = []
    # job_list.append(Job("Test_001", 10))
    # job_list.append(Job("Test_002", 9))
    # job_list.append(Job("Test_003", 8))
    # job_list.append(Job("Test_004", 7))
    # job_list.append(Job("Test_005", 6))
    # job_list.append(Job("Test_006", 5))
    # job_list.append(Job("Test_007", 4))
    # job_list.append(Job("Test_008", 3))
    # job_list.append(Job("Test_009", 2))
    # job_list.append(Job("Test_010", 1))

    # job_list.append(Job("Test_011", 10, job_list[0]))
    # job_list.append(Job("Test_012", 4, job_list[0]))
    # job_list.append(Job("Test_013", 7, job_list[0]))
    # job_list.append(Job("Test_014", 1, job_list[0]))
    # job_list.append(Job("Test_015", 3, job_list[0]))

    # job_list.append(Job("Test_016", 3, job_list[9]))
    # job_list.append(Job("Test_017", 2, job_list[9]))
    # job_list.append(Job("Test_018", 1, job_list[9]))

    job_list.append(Job("Test_001", "M01", ["R01_01", "R01_02", "R01_03", "R01_04", "R01_05", \
                                            "R01_06", "R01_07", "R01_08", "R01_09", "R01_10"]))
    job_list.append(Job("Test_002", "M01", ["R01_01", "R01_02", "R01_03", "R01_04", "R01_05", \
                                            "R01_06", "R01_07", "R01_08", "R01_09"]))
    job_list.append(Job("Test_003", "M01", ["R01_01", "R01_02", "R01_03", "R01_04", "R01_05", \
                                            "R01_06", "R01_07", "R01_08"]))
    job_list.append(Job("Test_004", "M01", ["R01_01", "R01_02", "R01_03", "R01_04", "R01_05", \
                                            "R01_06", "R01_07"]))
    job_list.append(Job("Test_005", "M01", ["R01_01", "R01_02", "R01_03", "R01_04", "R01_05", \
                                            "R01_06"]))
    job_list.append(Job("Test_006", "M01", ["R01_01", "R01_02", "R01_03", "R01_04", "R01_05"]))
    job_list.append(Job("Test_007", "M01", ["R01_07", "R01_08", "R01_09", "R01_10"]))
    job_list.append(Job("Test_008", "M01", ["R01_04", "R01_05", "R01_06"]))
    job_list.append(Job("Test_009", "M01", ["R01_02", "R01_03"]))
    job_list.append(Job("Test_010", "M01", ["R01_01"]))

    job_list.append(Job("Test_011", "M02", ["R02_01", "R02_02", "R02_03", "R02_04", "R02_05", \
                                            "R02_06", "R02_07", "R02_08", "R02_09", "R02_10"], \
                                            job_list[0]))
    job_list.append(Job("Test_012", "M02", ["R02_01", "R02_02", "R02_03", "R02_04"], job_list[0]))
    job_list.append(Job("Test_013", "M02", ["R02_01", "R02_02", "R02_03", "R02_04", "R02_05", \
                                            "R02_06", "R02_07"], job_list[0]))
    job_list.append(Job("Test_014", "M02", ["R02_01"], job_list[0]))
    job_list.append(Job("Test_015", "M02", ["R02_01", "R02_02", "R02_03"], job_list[0]))

    job_list.append(Job("Test_011", "M02", ["R02_01", "R02_02", "R02_03", "R02_04", "R02_05", \
                                            "R02_06", "R02_07", "R02_08", "R02_09", "R02_10"]))
    job_list.append(Job("Test_012", "M02", ["R02_01", "R02_02", "R02_03", "R02_04"]))
    job_list.append(Job("Test_013", "M02", ["R02_01", "R02_02", "R02_03", "R02_04", "R02_05", \
                                            "R02_06", "R02_07"]))
    job_list.append(Job("Test_014", "M02", ["R02_01"]))
    job_list.append(Job("Test_015", "M02", ["R02_01", "R02_02", "R02_03"]))

    job_list.append(Job("Test_016", "M03", ["R03_01", "R03_02", "R03_03"]))
    job_list.append(Job("Test_017", "M03", ["R03_01", "R03_02"]))
    job_list.append(Job("Test_018", "M03", ["R03_01"]))

    job_tree_builder = JobTree(job_list)

    job_tree_builder.build()
    #show(job_tree)
    schedulers = []
    for machine in job_tree_builder.machines:
        scheduler = Scheduler(machine)
        schedulers.append(scheduler)
    for scheduler in schedulers:
        scheduler.run()
    for scheduler in schedulers:
        scheduler.join()


