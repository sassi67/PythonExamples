""" Module containing const class """
##############################################################################
#  File      : const.py
#  Created   : Thu 02 Feb 12:31:00 2017
#  Author    : Alessandro Sacilotto, DCS Computing GmbH
#  Project   : Testharness
#  Copyright : 2013-2017
#  Version   : 1.0
##############################################################################
import sys

class _const(object):
    """ a class to generate constants """
    class ConstError(TypeError):
        """ a private class to avoid the reassignment of the constant value """
        pass
    def __setattr__(self, name, value):
        if self.__dict__.has_key(name):
            raise self.ConstError, "Can't rebind const(%s)" % name
        self.__dict__[name] = value

sys.modules[__name__] = _const()
