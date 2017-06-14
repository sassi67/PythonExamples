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
    def __init__(self, job_id, machine_name, required_resources, job_parent=None):
        """ constructor """
        self._job_id = job_id
        if not isinstance(self._job_id, basestring):
            raise "job_id is NOT a string"
        self._machine_name = machine_name
        if not isinstance(self._machine_name, basestring):
            raise "machine_name is NOT a string"
        self._required_resources = required_resources
        if not isinstance(self._required_resources, list):
            raise "required_resources is NOT a list"
        self._parent = job_parent
        if job_parent:
            if not isinstance(self._parent, Job):
                raise "job_parent is NOT a Job"
        self._status = JobState.NOT_TESTED
        self._children = [] # a list of Job
        self._next = None # or a Job

    # name, machine_name, required_resources are read-only
    @property
    def job_id(self):
        """ read the job name """
        return self._job_id

    @property
    def machine_name(self):
        """ read the machine name """
        return self._machine_name

    @property
    def resources(self):
        """ read the list of resources needed """
        return self._required_resources

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
        if len(self._required_resources) < len(other._required_resources):
            return True
        elif len(self._required_resources) > len(other._required_resources):
            return False
        else:
            return self._job_id < other.job_id

    # redefine 'less equal' operator
    def __le__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if len(self._required_resources) <= len(other._required_resources):
            return True
        elif len(self._required_resources) > len(other._required_resources):
            return False
        else:
            return self._job_id <= other.job_id

    # redefine 'greater' operator
    def __gt__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if len(self._required_resources) > len(other._required_resources):
            return True
        elif len(self._required_resources) < len(other._required_resources):
            return False
        else:
            return self._job_id > other.job_id

    # redefine 'greater equal' operator
    def __ge__(self, other):
        if not isinstance(other, Job):
            raise "Invalid comparison!"
        if len(self._required_resources) >= len(other._required_resources):
            return True
        elif len(self._required_resources) < len(other._required_resources):
            return False
        else:
            return self._job_id >= other.job_id
