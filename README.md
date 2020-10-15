# Log_Analyser
Using Big Data Technologies to analyse application logs as per requirement

Maximo Application when deployed in a clustered environment and functioning on multiple jvms can lead to generation of many log files everyday. These log files based on logging level can range from 5 MB-800 MB in size. Based on our requirement and need to debug certain issues, we may need to go through these log files. But going through any 100 mb log file even though for a particular time duration can be draining. This is because a lot of services write to log files simultaneously. For a need for easy extraction and saving manual hours I came up with using Spark application to perform this task.

Objective : In the attached spark job, I have tried to extract SQL statements logged in the application that take more than 3 seconds to execute.
Outcome : Using this Data , we can extract the sql statements in excel sheet , sorted in the order of maximum time taken. This order will help us to identify most resource intensive SQL statement which in turn can point us in the direction of optimising SQL statements, hence improving the performance of the system

About Spark:

