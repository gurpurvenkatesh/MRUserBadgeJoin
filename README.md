# MRUserBadgeJoin
MapReduce Application which needed to process 2 data sets using 2 mappers. And the mapper output will be processed by one reducer. 


Problem Statement : Issue :  
      Given is a User Access log file dataset that has user_id and his login_time into a particular system. Design a program that will accept start time and end time from the user. The program should tell me the list of all the users and the number of times each user logged in during the given time period.

Hadoop Command : hadoop jar /home/cloudera/Desktop/venkatesh/MRCSUserLog.jar MRUserLogs /user/cloudera/venkatesh/MRCSUserLogs/input/Users.xml /user/cloudera/venkatesh/MRCSUserLogs/input/Badges.xml /user/cloudera/venkatesh/MRCSUserLogs/Output3 2014-06-18T18:45:24.503 2015-06-18T18:45:24.503
