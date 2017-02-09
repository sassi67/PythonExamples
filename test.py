""" Test Module """
from job import Job, JobState
from jobtree import JobTree
from scheduler import Scheduler

results = ["NOT_TESTED", "SCHEDULED", "RUNNING", "OK", "FAILED", "SKIPPED"]
def show(alist):
    """ simple printer """
    for job in alist:
        print "%s --- > %s" % (job.name, results[job.status])
        show(job.children)

if __name__ == '__main__':
    
    RESOURCES_AVAILABLE = 20
    job_list = []
    job_list.append(Job("Test_001", 10))
    job_list.append(Job("Test_002", 9))
    job_list.append(Job("Test_003", 8))
    job_list.append(Job("Test_004", 7))
    job_list.append(Job("Test_005", 6))
    job_list.append(Job("Test_006", 5))
    job_list.append(Job("Test_007", 4))
    job_list.append(Job("Test_008", 3))
    job_list.append(Job("Test_009", 2))
    job_list.append(Job("Test_010", 1))

    job_list.append(Job("Test_011", 10, job_list[0]))
    job_list.append(Job("Test_012", 4, job_list[0]))
    job_list.append(Job("Test_013", 7, job_list[0]))
    job_list.append(Job("Test_014", 1, job_list[0]))
    job_list.append(Job("Test_015", 3, job_list[0]))

    job_list.append(Job("Test_016", 3, job_list[9]))
    job_list.append(Job("Test_017", 2, job_list[9]))
    job_list.append(Job("Test_018", 1, job_list[9]))

    job_tree_builder = JobTree(job_list, RESOURCES_AVAILABLE)

    job_tree = job_tree_builder.build_job_tree()
    show(job_tree)

    sched = Scheduler(RESOURCES_AVAILABLE, job_tree)
    sched.run()

    show(job_tree)


