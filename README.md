# PySpark
The basics of Apache Spark using Python

Install a JDK (Java Development Kit) from http://www.oracle.com/technetwork/java/javase/downloads/index.html . You must install the JDK into a path with no spaces, for example c:\jdk. Be sure to change the default location for the installation! DO NOT INSTALL JAVA 16. SPARK IS ONLY COMPATIBLE WITH JAVA 8 OR 11. 

Download a pre-built version of Apache Spark 3 from https://spark.apache.org/downloads.html 

If necessary, download and install WinRAR so you can extract the .tgz file you downloaded. http://www.rarlab.com/download.htm 

Extract the Spark archive, and copy its contents into C:\spark after creating that directory. You should end up with directories like c:\spark\bin, c:\spark\conf, etc. 

Download winutils.exe from https://sundog–s3.amazonaws.com/winutils.exe and move it into a C:\winutils\bin folder that you’ve created. (note, this is a 64-bit application. If you are on a 32-bit version of Windows, you’ll need to search for a 32-bit build of winutils.exe for Hadoop.) 

Create a c:\tmp\hive directory, and cd into c:\winutils\bin, and run winutils.exe chmod 777 c:\tmp\hive 

Open the the c:\spark\conf folder, and make sure “File Name Extensions” is checked in the “view” tab of Windows Explorer. Rename the log4j.properties.template file to log4j.properties. Edit this file (using Wordpad or something similar) and change the error level from INFO to ERROR for log4j.rootCategory 

Right-click your Windows menu, select Control Panel, System and Security, and then System. Click on “Advanced System Settings” and then the “Environment Variables” button. 

Add the following new USER variables: 

SPARK_HOME c:\spark 

JAVA_HOME (the path you installed the JDK to in step 1, for example C:\JDK) 

HADOOP_HOME c:\winutils 

PYSPARK_PYTHON python 

Add the following paths to your PATH user variable: 

%SPARK_HOME%\bin 

%JAVA_HOME%\bin 

Close the environment variable screen and the control panels. 

Install the latest Anaconda for Python 3 from anaconda.com. Don’t install a Python 2.7 version! If you already use some other Python environment, that’s OK – you can use it instead, as long as it is a Python 3 environment. 

Test it out! 

Open up your Start menu and select “Anaconda Prompt” from the Anaconda3 menu. 

Enter cd c:\spark and then dir to get a directory listing. 

Look for a text file we can play with, like README.md or CHANGES.txt 

Enter pyspark 

At this point you should have a >>> prompt. If not, double check the steps above. 

Enter rdd = sc.textFile(“README.md”) (or whatever text file you’ve found) Enter rdd.count() 

You should get a count of the number of lines in that file! Congratulations, you just ran your first Spark program! 

Enter quit() to exit the spark shell, and close the console window 

 
