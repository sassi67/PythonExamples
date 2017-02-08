import time
import datetime
start_time = datetime.datetime.now()
five_sec = datetime.timedelta(seconds=1) * 5

print "Start time: %s" % (start_time)
while True:
    time.sleep(1)
    if datetime.datetime.now() >= start_time + five_sec:
        break
print "End time: %s" % (datetime.datetime.now())
 
