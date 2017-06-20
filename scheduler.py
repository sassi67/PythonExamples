""" Module containing Scheduler class """
##############################################################################
#  File      : scheduler.py
#  Created   : Thu 07 Feb 07:08:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################
from threading import Thread, Semaphore, Lock, Timer, Event
from Queue import Queue
from time import sleep
from datetime import timedelta
import datetime as dt
import logging

from jobtree import JobTree, Machine, JobResource
from job import Job, JobState
from threadpool import Worker, ThreadPool

class Scheduler(object):
    """ job scheduler """
    def __init__(self, machine, timeout=3600):
        """" constructor """
        self._machine = machine
        if not isinstance(self._machine, Machine):
            raise "machine is NOT a Machine object"
        self._timeout = timeout
        if not isinstance(self._timeout, int):
            raise "timeout is NOT an integer"
        # thread pool for producer/consumer
        self._tp_prod_cons = ThreadPool(2)
        # semaphores
        self._free_res_sem = None
        self._used_res_sem = None
        # queue for exchange
        self._job_buffer = Queue()
        # thread pool for the resource management
        self._tp_resources = ThreadPool(len(self._machine.resources))
        self._resources_available = len(self._machine.resources)
        # descriptions
        self._results = ["NOT_TESTED", "SCHEDULED", "RUNNING", "OK", "FAILED", "SKIPPED"]
        #
        self._mutex = Lock()
        self._timeout_timers = {} # a dictionary of Job -> (Timer, Event)
        self._job_timers = {} # a dictionary of Job -> (Timer, Event)
        # TODO: only for test --> remove from production!!
        self._job_tests = {}

    def run(self):
        """ start the scheduler """
        # semaphore managed by the producer
        self._free_res_sem = Semaphore(len(self._machine.resources))
        # semaphore managed by the consumer
        self._used_res_sem = Semaphore(0)
        # threads creation for producer/consumer
        self._tp_prod_cons.add_task(func=self._producer)
        self._tp_prod_cons.add_task(func=self._consumer)

    def join(self):
        """ join the threads """
        self._tp_prod_cons.wait_completion()
        self._job_buffer.join()

    def _get_resources_available(self):
        n_res = 0
        with self._mutex:
            n_res = self._resources_available
        return n_res

    def _set_resources_available(self, n_res):
        with self._mutex:
            self._resources_available = n_res

    def _job_timeout(self, job):
        (_, timeout_evt) = self._timeout_timers[job]
        timeout_evt.set() # signal to method _job_executed the timeout happened
        # TODO: only for test --> remove from production!!
        self._job_tests[job].cancel()

    def _job_executed(self, job, time):
        (timeout_tmr, timeout_evt) = self._timeout_timers[job]
        (job_tmr, job_evt) = self._job_timers[job]
        timeout_is_set = False
        # wait the event job is finished
        while not job_evt.isSet():
            timeout_is_set = timeout_evt.isSet()
            if timeout_is_set:
                job_tmr.cancel()
                break
            evt_is_set = job_evt.wait(time)
            if evt_is_set:
                timeout_tmr.cancel()

        self._job_end(job, timeout_is_set)

    # TODO: only for test --> remove from production!!
    def _job_test_run(self, job):
        (_, job_evt) = self._job_timers[job]
        job_evt.set()
    #==================================================

    def _job_end(self, job, timeout_is_set):
        logging.info("Consumer %s - Job: %s -- End time" % \
                    (self._machine.name, job.job_id))
        #-------------------
        self._job_buffer.task_done()
        # free the resources
        self._set_resources_busy(job, False)
        for _ in range(len(job.resources)):
            self._used_res_sem.acquire()
            n_res = self._get_resources_available() + 1
            self._set_resources_available(n_res)
            self._free_res_sem.release()
        # get the result of the test on this job
        if timeout_is_set:
            job.status = JobState.TEST_FAIL
        else:
            job.status = JobState.TEST_OK
        # TODO: remove the following 'if' in production
        # FOR TEST: put always
        if job.job_id == "Test_010":
            job.status = JobState.TEST_FAIL
        else:
            job.status = JobState.TEST_OK
        # =================================
        logging.info("Consumer %s - Job: %s executed with result -> %s." % \
        (self._machine.name, job.job_id, self._results[job.status]))
        logging.info("Consumer %s - Job: %s resources released -> %d." % \
        (self._machine.name, job.job_id, len(job.resources)))
        logging.info("Consumer %s - Resources available now: %d." % \
        (self._machine.name, self._get_resources_available()))
        #-------------------

        # if the test fails the put all the job children in the TEST_SKIPPED state
        if job.status == JobState.TEST_FAIL:
            self._skip_children(job)

        # clear the dictionaries
        del self._timeout_timers[job]
        del self._job_timers[job]
        # TODO: only for test --> remove from production!!
        del self._job_tests[job]
        #==================================================

    def _job_run(self, job):
        # create a timeout timer for this job
        self._timeout_timers[job] = (Timer(interval=self._timeout, \
                                           function=self._job_timeout, \
                                           args=[job]), \
                                     Event())
        (timeout_tmr, _) = self._timeout_timers[job]
        timeout_tmr.start()
        logging.info("Consumer %s - Job: %s -- Timeout timer started" % \
                    (self._machine.name, job.job_id))
        # create an event for this job to signal the end of its execution
        self._job_timers[job] = (Timer(interval=1, \
                                       function=self._job_executed, \
                                       args=[job, 1]), \
                                 Event())
        (job_tmr, _) = self._job_timers[job]
        job_tmr.start()
        logging.info("Consumer %s - Job: %s -- Start time" % \
                    (self._machine.name, job.job_id))
        # TODO: only for test --> remove from production!!
        self._job_tests[job] = Timer(interval=5, function=self._job_test_run, args=[job])
        self._job_tests[job].start()
        #==================================================

    def _are_resources_busy(self, job):
        # check if all the resources related to this job are free
        res_busy = False
        with self._mutex:
            for res_job in job.resources:
                for res_machine in self._machine.resources:
                    if res_job == res_machine.name:
                        if res_machine.locked:
                            res_busy = True
                            break
                if res_busy:
                    break
        return res_busy

    def _set_resources_busy(self, job, busy):
        # set the lock state for job
        with self._mutex:
            for res_job in job.resources:
                for res_machine in self._machine.resources:
                    if res_job == res_machine.name:
                        res_machine.locked = busy

    def _get_next_job(self, job):
        next_job = None
        # if the parent job has not finished the test  then skip
        if job.parent:
            if job.parent.status != JobState.TEST_OK:
                return next_job
            else: # or in a different machine
                if job.parent.machine_name != job.parent.machine_name:
                    if job.children:
                        next_job = self._get_next_job(job.children[0])
                    if job.next:
                        next_job = self._get_next_job(job.next)

        if job.status == JobState.NOT_TESTED and \
            len(job.resources) <= self._get_resources_available():
            if not self._are_resources_busy(job):
                self._set_resources_busy(job, True)
                next_job = job
        if next_job is None and job.children:
            next_job = self._get_next_job(job.children[0])
        if next_job is None and job.next:
            next_job = self._get_next_job(job.next)
        return next_job

    def _job_not_scheduled(self, job):
        job_not_scheduled = (job.status == JobState.NOT_TESTED)
        if not job_not_scheduled and job.children:
            job_not_scheduled = self._job_not_scheduled(job.children[0])
        if not job_not_scheduled and job.next:
            job_not_scheduled = self._job_not_scheduled(job.next)
        return job_not_scheduled

    def _producer(self):
        """ get the next job and send it to the buffer """
        stop_producer = False
        while True:
            # if there is at least one resource available
            if self._get_resources_available() > 0:
                # search the next job
                next_job = self._get_next_job(self._machine.jobs[0])
                if next_job:
                    logging.info("Producer %s: scheduled the job %s resources needed: %d" % \
                    (self._machine.name, next_job.job_id, len(next_job.resources)))
                    next_job.status = JobState.SCHEDULED
                    for _ in range(len(next_job.resources)):
                        self._free_res_sem.acquire()
                        n_res = self._get_resources_available() - 1
                        self._set_resources_available(n_res)
                        self._used_res_sem.release()
                    logging.info("Producer %s: Resources still available: %d." % \
                    (self._machine.name, self._get_resources_available()))
                    # put the job in the buffer
                    self._job_buffer.put(next_job)
                else:
                    # are there jobs not tested yet? If not stop then stop Producer
                    if not self._job_not_scheduled(self._machine.jobs[0]):
                        stop_producer = True
            if stop_producer:
                logging.info("Producer %s: finished." % (self._machine.name))
                break
            sleep(0.01)

    def _job_not_executed(self, job):
        job_not_executed = (job.status < JobState.TEST_OK)
        if not job_not_executed and job.children:
            job_not_executed = self._job_not_executed(job.children[0])
        if not job_not_executed and job.next:
            job_not_executed = self._job_not_executed(job.next)
        return job_not_executed

    def _skip_children(self, parent_job):
        for child_job in parent_job.children:
            child_job.status = JobState.TEST_SKIPPED
            logging.info("Consumer %s - Job: %s marked as %s." % \
            (self._machine.name, child_job.job_id, self._results[child_job.status]))
            self._skip_children(child_job)

    def _consumer(self):
        """ get the next scheduled job from the buffer and execute it """
        stop_consumer = False
        while True:
            if not self._job_buffer.empty():
                # get the job from the buffer
                job = self._job_buffer.get()
                job.status = JobState.RUNNING
                logging.info("Consumer %s: the job %s is sent to execution." \
                                % (self._machine.name, job.job_id))
                # add the job to the thread pool
                self._tp_resources.add_task(self._job_run, job)
            else:
                # are all the jobs tested? If yes then stop Consumer
                if not self._job_not_executed(self._machine.jobs[0]):
                    stop_consumer = True
            if stop_consumer:
                logging.info("Consumer %s: finished." % (self._machine.name))
                break
            sleep(0.01)
        self._tp_resources.wait_completion()
