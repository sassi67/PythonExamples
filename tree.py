""" Module containing Tree class """
##############################################################################
#  File      : tree.py
#  Created   : Thu 02 Feb 12:31:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################
from job import Job

class Tree(object):
    """ class responsible for the creation of the queue for jobs """
    def __init__(self, job_list, resources_available):
        """ constructor """
        self.job_list = job_list
        if not isinstance(self.job_list, list(Job)):
            print "job_list is NOT a list"
            exit(1)
        self.resources_available = resources_available
        if not isinstance(self.resources_available, int):
            print "resources_available is NOT an int"
            exit(1)
