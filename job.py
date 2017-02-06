""" Module containing Job class """
##############################################################################
#  File      : job.py
#  Created   : Thu 02 Feb 12:31:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################

def enum(**enums):
    """ old style build enumeration """
    return type('Enum', (), enums)

JobState = enum(
    NOT_TESTED=0,
    RUNNING=1,
    TEST_OK=2,
    TEST_FAIL=3,
    TEST_SKIPPED=4
)

class Job(object):
    """ Fake class to represent a job """
    def __init__(self, job_name, resources_needed, parentjob=None):
        """ constructor """
        self._job_name = job_name
        if not isinstance(self._job_name, basestring):
            raise "job_name is NOT a string"
        self._resources_needed = resources_needed
        if not isinstance(self._resources_needed, int):
            raise "resources_needed is NOT an int"
        self._parent_job = parentjob
        if parentjob:
            if not isinstance(self._parent_job, Job):
                raise "parentjob is NOT a Job"
        self._job_status = JobState.NOT_TESTED
        self._children = [] # a list of Job
        self._next = None # or a Job

    # job_name, resources_needed are read-only
    @property
    def job_name(self):
        """ read the job name """
        return self._job_name

    @property
    def resources_needed(self):
        """ read the number of resources needed """
        return self._resources_needed

    @property
    def parent_job(self):
        """ read the parent job """
        return self._parent_job
    @parent_job.setter
    def parent_job(self, job):
        """ set the parent job """
        if not isinstance(job, Job):
            raise "job is NOT a Job"
        self._parent_job = job

    @property
    def job_status(self):
        """ read the job status """
        return self._job_status
    @job_status.setter
    def job_status(self, status):
        """ change the job status """
        if status >= JobState.NOT_TESTED and status <= JobState.TEST_SKIPPED:
            self._job_status = status

    @property
    def children(self):
        """ read the list of its job children """
        return self._children

    def addchild(self, job):
        """ add a job to the children list """
        if not isinstance(job, Job):
            raise "job is NOT a Job"
        self._children.append(job)

    @property
    def next(self):
        """ read the next job children """
        return self._next

    def setnext(self, job):
        """ add a job to the children list """
        if not isinstance(job, Job):
            raise "job is NOT a Job"
        self._next = job

    # redefine 'less' operator
    def __lt__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources_needed < other.resources_needed:
            return True
        elif self._resources_needed > other.resources_needed:
            return False
        else:
            self._job_name < other.job_name

    # redefine 'less equal' operator
    def __le__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources_needed < other.resources_needed:
            return True
        elif self._resources_needed > other.resources_needed:
            return False
        else:
            self._job_name <= other.job_name

    # redefine 'greater' operator
    def __gt__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources_needed > other.resources_needed:
            return True
        elif self._resources_needed < other.resources_needed:
            return False
        else:
            self._job_name > other.job_name

    # redefine 'greater equal' operator
    def __ge__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources_needed > other.resources_needed:
            return True
        elif self._resources_needed < other.resources_needed:
            return False
        else:
            self._job_name >= other.job_name
