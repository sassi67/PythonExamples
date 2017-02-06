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
    def __init__(self, job_list, resources_available):
        """ constructor """
        self._job_list = job_list
        if not isinstance(self._job_list, list):
            raise "job_list is NOT a list"
        self.resources_available = resources_available
        if not isinstance(self.resources_available, int):
            raise "resources_available is NOT an int"
        self._job_tree = []

    def build_job_tree(self):
        """ build the tree and return it"""
        # loop the initial list of jobs and foreach job create a new sublist of its children
        for pjob in self._job_list:
            childrenlist = []
            for job in self._job_list:
                if pjob == job:
                    continue
                if job.parent_job == pjob:
                    childrenlist.append(job)
            if len(childrenlist) > 0:
                # order the sublist of children
                quick_sort(childrenlist)
                # append the order list of children to their parent
                for job in childrenlist:
                    pjob.addchild(job)
        # build a sublist of all the jobs without parents and sort it
        for job in self._job_list:
            if job.parent_job:
                continue
            self._job_tree.append(job)
        quick_sort(self._job_tree)

        return self._job_tree

    @property
    def job_tree(self):
        """ read the tree of jobs """
        return self._job_tree

if __name__ == '__main__':
    L = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
    quick_sort(L)
    for n in L:
        print n
