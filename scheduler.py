""" Module containing Scheduler class """
##############################################################################
#  File      : scheduler.py
#  Created   : Thu 07 Feb 07:08:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################
from threading import Thread, Semaphore
from Queue import Queue
from time import sleep
import datetime as dt
from datetime import timedelta

from jobtree import JobTree
from job import Job, JobState
from threadpool import Worker, ThreadPool

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
        self._job_buffer = Queue()
        # thread pool
        self._pool = ThreadPool(total_resources)
        # thread pool for the synchronous management of messages
        self._msg_manager = ThreadPool(1)
        # descriptions
        self._results = ["NOT_TESTED", "SCHEDULED", "RUNNING", "OK", "FAILED", "SKIPPED"]

    def run(self):
        """ start the scheduler """
        # semaphores initialization
        # semaphore managed by the producer
        self._free_res_sem = Semaphore(self._total_resources)
        # semaphore managed by the consumer
        self._used_res_sem = Semaphore(0)
        # threads creation
        thProd = Thread(target=self.__producer)
        thCons = Thread(target=self.__consumer)
        thProd.start()
        thCons.start()
        thProd.join()
        thCons.join()
        self._job_buffer.join()

    def __print_msg_private(self, msg):
        print "%s --> %s" % (dt.datetime.now(), msg)

    def __print_msg(self, msg):
        self._msg_manager.add_task(self.__print_msg_private, msg)

    def __job_run(self, job):
        start_time = dt.datetime.now()
        five_sec = timedelta(seconds=1) * 5

        self.__print_msg("     Job: %s -- Start time: %s" % (job.job_name, start_time))
        while True:
            sleep(1)
            if dt.datetime.now() >= start_time + five_sec:
                break
        self.__print_msg("     Job: %s -- End time: %s" % (job.job_name, dt.datetime.now()))
        #-------------------
        self._job_buffer.task_done()
        for _ in range(0, job.resources_needed):
            self._used_res_sem.acquire()
            self._resources_available = self._resources_available + 1
            self._free_res_sem.release()
        # ----------------
        # get the result of the test on this job
        # FOR TEST: put always
        if job.job_name == "Test_010":
            job.job_status = JobState.TEST_FAIL
        else:
            job.job_status = JobState.TEST_OK
        self.__print_msg("     Job: %s executed with result -> %s." % \
        (job.job_name, self._results[job.job_status]))
        self.__print_msg("     Job: %s resources released -> %d." % \
        (job.job_name, job.resources_needed))
        self.__print_msg("     Job: Resources available now: %d." % \
        (self._resources_available))
        # if the test fails the put all the job children in the TEST_SKIPPED state
        if job.job_status == JobState.TEST_FAIL:
            self.__skip_children(job)

    def __get_next_job(self, job):
        next_job = None
        # if the parent job has not finished the test then skip
        if job.parent_job:
            if job.parent_job.job_status != JobState.TEST_OK:
                return next_job
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
        stop_producer = False
        while True:
            # if there is at least one resource available
            if self._resources_available > 0:
                # search the next job
                next_job = self.__get_next_job(self._job_tree[0])
                if next_job:
                    self.__print_msg("Producer: scheduled the job -> %s resources needed: %d" % \
                    (next_job.job_name, next_job.resources_needed))
                    next_job.job_status = JobState.SCHEDULED
                    for _ in range(0, next_job.resources_needed):
                        self._free_res_sem.acquire()
                        self._resources_available = self._resources_available - 1
                        self._used_res_sem.release()
                    self.__print_msg("Producer: Resources still available: %d." % \
                    (self._resources_available))
                    # put the job in the buffer
                    self._job_buffer.put(next_job)
                else:
                    # are there jobs not tested yet? If not stop then stop Producer
                    if not self.__job_not_scheduled(self._job_tree[0]):
                        stop_producer = True
            if stop_producer:
                self.__print_msg("Producer: finished.")
                break
            sleep(1)

    def __job_not_executed(self, job):
        job_not_executed = (job.job_status < JobState.TEST_OK)
        if not job_not_executed and job.children:
            job_not_executed = self.__job_not_executed(job.children[0])
        if not job_not_executed and job.next:
            job_not_executed = self.__job_not_executed(job.next)
        return job_not_executed

    def __skip_children(self, parent_job):
        for child_job in parent_job.children:
            child_job.job_status = JobState.TEST_SKIPPED
            self.__print_msg("     Job: %s marked as %s." % \
            (child_job.job_name, self._results[child_job.job_status]))
            self.__skip_children(child_job)

    def __consumer(self):
        """ get the next scheduled job from the buffer and execute it """
        stop_consumer = False
        while True:
            if not self._job_buffer.empty():
                # get the job from the buffer
                job = self._job_buffer.get()
                job.job_status = JobState.RUNNING
                self.__print_msg("Consumer: the job -> %s is sent to execution." % (job.job_name))
                # add the job to the thread pool
                self._pool.add_task(self.__job_run, job)
            else:
                # are all the jobs tested? If yes then stop Consumer
                if not self.__job_not_executed(self._job_tree[0]):
                    stop_consumer = True
            if stop_consumer:
                self.__print_msg("Consumer: finished.")
                break
            sleep(1)
        self._pool.wait_completion()
        self._msg_manager.wait_completion()
