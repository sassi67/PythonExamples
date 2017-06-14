""" Module containing JobTree class """
##############################################################################
#  File      : jobtree.py
#  Created   : Thu 02 Feb 12:31:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################
from job import Job

def quick_sort(alist):
    """ quicksort main function """
    quick_sort_helper(alist, 0, len(alist) - 1)

def quick_sort_helper(alist, first, last):
    """ quicksort helper function"""
    if first < last:
        splitpoint = partition(alist, first, last)
        quick_sort_helper(alist, first, splitpoint - 1)
        quick_sort_helper(alist, splitpoint + 1, last)

def partition(alist, first, last):
    """ partition builder (helper function)"""
    pivotvalue = alist[first]
    leftmark = first + 1
    rightmark = last
    done = False
    while not done:
        while leftmark <= rightmark and alist[leftmark] <= pivotvalue:
            leftmark = leftmark + 1
        while alist[rightmark] >= pivotvalue and rightmark >= leftmark:
            rightmark = rightmark - 1
        if rightmark < leftmark:
            done = True
        else:
            temp = alist[leftmark]
            alist[leftmark] = alist[rightmark]
            alist[rightmark] = temp
    temp = alist[first]
    alist[first] = alist[rightmark]
    alist[rightmark] = temp
    return rightmark

class JobTree(object):
    """ class responsible for the creation of the queue for jobs """
    def __init__(self, job_list):
        """ constructor """
        self._job_list = job_list
        if not isinstance(self._job_list, list):
            raise "job_list is NOT a list"
        self._machines = [] # a list of Machine(s)
        self._job_tree = [] # a list of Job(s)

    def build(self):
        """ build the tree """
        for job in self._job_list:
            existing_machine = False
            for machine in self._machines:
                if job.machine_name == machine.name:
                    existing_machine = True
                    break
            if not existing_machine:
                # create a new Machine
                machine = Machine(job.machine_name)
            # add the resources
            for resource in job.resources:
                machine.add_resource(resource)
            # add this job
            machine.add_job(job)
            if not existing_machine:
                self._machines.append(machine)
        # order the jobs for each machine
        for machine in self._machines:
            quick_sort(machine.jobs)
        # loop the initial list of jobs and foreach job create a new sublist of its children
        for pjob in self._job_list:
            childrenlist = []
            for job in self._job_list:
                if pjob == job:
                    continue
                if job.parent == pjob:
                    childrenlist.append(job)
            if len(childrenlist) > 0:
                # order the sublist of children
                quick_sort(childrenlist)
                # set the "next" attribute of each element in the list
                for i, _ in reversed(list(enumerate(childrenlist))):
                    if i < len(childrenlist) - 1:
                        childrenlist[i].setnext(childrenlist[i + 1])
                # append the ordered list of children to their parent
                for job in childrenlist:
                    pjob.addchild(job)
        # build a sublist of all the jobs without parents and sort it
        for job in self._job_list:
            if job.parent:
                continue
            self._job_tree.append(job)
        quick_sort(self._job_tree)
        # set the "next" attribute of each element in the list
        for i, _ in reversed(list(enumerate(self._job_tree))):
            if i < len(self._job_tree) - 1:
                self._job_tree[i].setnext(self._job_tree[i + 1])

    @property
    def machines(self):
        """ read the tree of jobs """
        return self._machines
    @property
    def job_tree(self):
        """ read the tree of jobs """
        return self._job_tree

class JobResource(object):
    """ Class to represent a resource """
    def __init__(self, name):
        self._name = name
        self._locked = False
    # name is read-only
    @property
    def name(self):
        """ read the job name """
        return self._name
    @property
    def locked(self):
        """ read the lock status """
        return self._locked
    @locked.setter
    def locked(self, lock):
        """ set the lock status """
        if not isinstance(lock, bool):
            raise "lock is NOT a bool"
        self._locked = lock

class Machine(object):
    """ Class to represent a Machine """
    def __init__(self, name):
        self._name = name
        self._resources = [] # a list of JobResource
        self._jobs = [] # a list of Job
    # name is read-only
    @property
    def name(self):
        """ read the job name """
        return self._name
    @property
    def resources(self):
        """ read the list of resources"""
        return self._resources
    def add_resource(self, resource):
        """ add a resource to the machine """
        resource_exists = False
        for res in self._resources:
            if res.name == resource:
                resource_exists = True
                break
        if not resource_exists:
            self._resources.append(JobResource(resource))

    @property
    def jobs(self):
        """ read the job tree"""
        return self._jobs
    def add_job(self, job):
        """ add a job to the machine """
        self._jobs.append(job)

if __name__ == '__main__':
    L = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
    quick_sort(L)
    for n in L:
        print n
