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
    SCHEDULED=1,
    RUNNING=2,
    TEST_OK=3,
    TEST_FAIL=4,
    TEST_SKIPPED=5
)

class Job(object):
    """ Fake class to represent a job """
    def __init__(self, job_name, job_resources, job_parent=None):
        """ constructor """
        self._name = job_name
        if not isinstance(self._name, basestring):
            raise "job_name is NOT a string"
        self._resources = job_resources
        if not isinstance(self._resources, int):
            raise "job_resources is NOT an int"
        self._parent = job_parent
        if job_parent:
            if not isinstance(self._parent, Job):
                raise "job_parent is NOT a Job"
        self._status = JobState.NOT_TESTED
        self._children = [] # a list of Job
        self._next = None # or a Job

    # name, job_resources are read-only
    @property
    def name(self):
        """ read the job name """
        return self._name

    @property
    def resources(self):
        """ read the number of resources needed """
        return self._resources

    @property
    def parent(self):
        """ read the parent job """
        return self._parent
    @parent.setter
    def parent(self, job):
        """ set the parent job """
        if not isinstance(job, Job):
            raise "job is NOT a Job"
        self._parent = job

    @property
    def status(self):
        """ read the job status """
        return self._status
    @status.setter
    def status(self, status):
        """ change the job status """
        if status >= JobState.NOT_TESTED and status <= JobState.TEST_SKIPPED:
            self._status = status

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
        if self._resources < other.resources:
            return True
        elif self._resources > other.resources:
            return False
        else:
            self._name < other.name

    # redefine 'less equal' operator
    def __le__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources < other.resources:
            return True
        elif self._resources > other.resources:
            return False
        else:
            self._name <= other.name

    # redefine 'greater' operator
    def __gt__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources > other.resources:
            return True
        elif self._resources < other.resources:
            return False
        else:
            self._name > other.name

    # redefine 'greater equal' operator
    def __ge__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if self._resources > other.resources:
            return True
        elif self._resources < other.resources:
            return False
        else:
            self._name >= other.name
