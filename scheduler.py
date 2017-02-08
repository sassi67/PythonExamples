""" Module containing Scheduler class """
##############################################################################
#  File      : scheduler.py
#  Created   : Thu 07 Feb 07:08:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################
import threading
import Queue
import time
import datetime

from jobtree import JobTree
from job import Job, JobState

class Scheduler(object):
    """ job scheduler """
    def __init__(self, total_resources, job_tree):
        """" constructor """
        self._total_resources = total_resources
        if not isinstance(self._total_resources, int):
            raise "total_resources is NOT an int"
        if self._total_resources < 1:
            raise "no resources available"
        self._resources_available = total_resources
        self._job_tree = job_tree
        if not isinstance(self._job_tree, list):
            raise "job_tree is NOT a list"
        # semaphores
        self._free_res_sem = None
        self._used_res_sem = None
        # queue for exchange
        self._job_buffer = Queue.Queue()
        #
        self._stop_producer = False

    def run(self):
        """ start the scheduler """
        # semaphores initialization
        # semaphore managed by the producer
        self._free_res_sem = threading.Semaphore(self._total_resources)
        # semaphore managed by the consumer
        self._used_res_sem = threading.Semaphore(0)
        # threads creation
        thProd = threading.Thread(target=self.__producer)
        thCons = threading.Thread(target=self.__consumer)
        thProd.start()
        thCons.start()
        thProd.join()
        thCons.join()

    def __job_run(self, job):
        start_time = datetime.datetime.now()
        five_sec = datetime.timedelta(seconds=1) * 5

        print "Start time %s: %s" % (job.job_name, start_time)
        while True:
            time.sleep(1)
            if datetime.datetime.now() >= start_time + five_sec:
                break
        print "End time %s: %s" % (job.job_name, datetime.datetime.now())
        job.job_status = JobState.TEST_OK

    def __get_next_job(self, job):
        next_job = None
        if job.job_status == JobState.NOT_TESTED and \
            job.resources_needed <= self._resources_available:
            next_job = job
        if next_job is None and job.children:
            next_job = self.__get_next_job(job.children[0])
        if next_job is None and job.next:
            next_job = self.__get_next_job(job.next)
        return next_job

    def __job_not_scheduled(self, job):
        job_not_scheduled = (job.job_status == JobState.NOT_TESTED)
        if not job_not_scheduled and job.children:
            job_not_scheduled = self.__job_not_scheduled(job.children[0])
        if not job_not_scheduled and job.next:
            job_not_scheduled = self.__job_not_scheduled(job.next)
        return job_not_scheduled

    def __producer(self):
        """ get the next job and send it to the buffer """
        while True:
            # if there is at least one resource available
            if self._resources_available > 0:
                # search the next job
                next_job = self.__get_next_job(self._job_tree[0])
                if next_job:
                    print "Producer has put in the queue the job: %s" % (next_job.job_name)
                    next_job.job_status = JobState.SCHEDULED
                    # put the job in the buffer
                    self._job_buffer.put(next_job)
                    for _ in range(0, next_job.resources_needed):
                        self._free_res_sem.acquire()
                        self._resources_available = self._resources_available - 1
                        self._used_res_sem.release()
                    print "There are still: %d resources available." % (self._resources_available)
                else:
                    # are there jobs not scheduled yet? If not stop then stop Producer
                    if self.__job_not_scheduled(self._job_tree[0]):
                        self._stop_producer = False
                    else:
                        self._stop_producer = True
            if self._stop_producer:
                print "Producer has finished"
                break
            time.sleep(1)

    def __consumer(self):
        """ get the next scheduled job from the buffer and execute it """
        while True:
            if not self._job_buffer.empty():
                # get the job from the buffer and run it
                next_job = self._job_buffer.get()
                next_job.job_status = JobState.RUNNING
                print "Consumer sends: %s to execution." % (next_job.job_name)
                # FOR TEST: wait 5 secs
                
                #-------------------
                self._job_buffer.task_done()
                for _ in range(0, next_job.resources_needed):
                    self._used_res_sem.acquire()
                    self._resources_available = self._resources_available + 1
                    self._free_res_sem.release()
                # ----------------
                # get the result of the test on this job
                # FOR TEST: put always
                next_job.job_status = JobState.TEST_OK
                print "Consumer has executed: %s with result -> %d" % \
                (next_job.job_name, next_job.job_status)
                print "There are now: %d resources available." % (self._resources_available)
                # if the test fails the put all the job children in the TEST_SKIPPED state
                # FIXME: a recursive function needed
                if next_job.job_status == JobState.TEST_FAIL:
                    for child in next_job:
                        child.job_status = JobState.TEST_SKIPPED
                # ----------------
            if self._stop_producer:
                print "Consumer has finished"
                break
            time.sleep(1)
        self._job_buffer.join()