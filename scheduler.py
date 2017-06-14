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
import datetime as dt
from datetime import timedelta

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
        # thread pool for the synchronous management of messages
        self._msg_manager = ThreadPool(1)
        # descriptions
        self._results = ["NOT_TESTED", "SCHEDULED", "RUNNING", "OK", "FAILED", "SKIPPED"]
        #
        self._mutex = Lock()
        self._timeout_event = Event()
        self._timeout_timer = None # a Timer

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

    def _print_msg_private(self, msg):
        print "%s --> %s" % (dt.datetime.now(), msg)

    def _print_msg(self, msg):
        self._msg_manager.add_task(self._print_msg_private, msg)

    def _is_timeout(self, job, event):
        event.set()
        self._job_end(job, event)

    def _job_end(self, job, event):
        timeout_is_set = False
        while not event.isSet():
            timeout_is_set = event.wait(self._timeout)

        self._print_msg("     Job: %s -- End time: %s" % (job.job_id, dt.datetime.now()))
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
        # TODO: remove the following 'if' in production
        # FOR TEST: put always
        if job.job_id == "Test_010":
            job.status = JobState.TEST_FAIL
        else:
            job.status = JobState.TEST_OK
        # =================================
        if timeout_is_set:
            job.status = JobState.TEST_FAIL
        else:
            job.status = JobState.TEST_OK

        self._print_msg("     Job: %s executed with result -> %s." % \
        (job.job_id, self._results[job.status]))
        self._print_msg("     Job: %s resources released -> %d." % \
        (job.job_id, len(job.resources)))
        self._print_msg("     Job: Resources available now: %d." % \
        (self._get_resources_available()))
        #-------------------

        # if the test fails the put all the job children in the TEST_SKIPPED state
        if job.status == JobState.TEST_FAIL:
            self._skip_children(job)

    def _job_run(self, job):
        #TODO: replace this part with the job execution
        start_time = dt.datetime.now()
        self._print_msg("     Job: %s -- Start time: %s" % (job.job_id, start_time))
        # five_sec = timedelta(seconds=1) * 5
        self._timeout_timer = Timer(interval=self._timeout, function=self._is_timeout, \
                                    args=[job, self._timeout_event])
        self._timeout_timer.start()
        self._print_msg("     Job: %s -- Timeout timer" % (job.job_id))

        # while True:
        #     sleep(1)
        #     if dt.datetime.now() >= start_time + five_sec:
        #         break

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
        # if the parent job has not finished the test then skip
        if job.parent:
            if job.parent.status != JobState.TEST_OK:
                return next_job
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
                    self._print_msg("Producer %s: scheduled the job -> %s resources needed: %d" % \
                    (self._machine.name, next_job.job_id, len(next_job.resources)))
                    next_job.status = JobState.SCHEDULED
                    for _ in range(len(next_job.resources)):
                        self._free_res_sem.acquire()
                        n_res = self._get_resources_available() - 1
                        self._set_resources_available(n_res)
                        self._used_res_sem.release()
                    self._print_msg("Producer %s: Resources still available: %d." % \
                    (self._machine.name, self._get_resources_available()))
                    # put the job in the buffer
                    self._job_buffer.put(next_job)
                else:
                    # are there jobs not tested yet? If not stop then stop Producer
                    if not self._job_not_scheduled(self._machine.jobs[0]):
                        stop_producer = True
            if stop_producer:
                self._print_msg("Producer %s: finished." % (self._machine.name))
                break
            sleep(1)

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
            self._print_msg("     Job: %s marked as %s." % \
            (child_job.name, self._results[child_job.status]))
            self._skip_children(child_job)

    def _consumer(self):
        """ get the next scheduled job from the buffer and execute it """
        stop_consumer = False
        while True:
            if not self._job_buffer.empty():
                # get the job from the buffer
                job = self._job_buffer.get()
                job.status = JobState.RUNNING
                self._print_msg("Consumer %s: the job -> %s is sent to execution." \
                                % (self._machine.name, job.job_id))
                # add the job to the thread pool
                self._tp_resources.add_task(self._job_run, job)
            else:
                # are all the jobs tested? If yes then stop Consumer
                if not self._job_not_executed(self._machine.jobs[0]):
                    stop_consumer = True
            if stop_consumer:
                self._print_msg("Consumer %s: finished." % (self._machine.name))
                break
            sleep(1)
        self._tp_resources.wait_completion()
        self._msg_manager.wait_completion()
