""" Test Module """
from job import Job, JobState
from jobtree import JobTree

def show(alist):
    """ simple printer """
    for job in alist:
        if job.parent_job:
            print "%s --- > %s" % (job.parent_job.job_name, job.job_name)
        else:
            print job.job_name
        show(job.children)

if __name__ == '__main__':
#j1 = Job("Test_001", 2)
#j2 = Job("Test_002", 1, j1)

#print j1.job_name
#print j1.resources_needed
#print j1.job_status

#j1.job_status = JobState.RUNNING

#print "New status: %d" % (j1.job_status)

#print "j2 name: %s" % (j2.job_name)
#print "j2 resources_needed: %d" % (j2.resources_needed)
#print "j2 job_status: %d" % (j2.job_status)

#children = j1.children
#for j in children:
#    print "child job name: %s" % (j.job_name)
#    print "parent job name: %s" % (j.parent_job.job_name)

#if j1 < j2:
#    print "j1 < j2"
#else:
#    print "j2 < j1"

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

    job_list.append(Job("Test_016", 3, job_list[11]))
    job_list.append(Job("Test_017", 2, job_list[11]))
    job_list.append(Job("Test_018", 1, job_list[11]))

    job_tree_builder = JobTree(job_list, 10)

    show(job_tree_builder.build_job_tree())

