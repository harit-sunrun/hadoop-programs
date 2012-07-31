#!/usr/bin/env python
"""
Run as : hadoop jar /usr/local/Cellar/hadoop/1.0.3/libexec/contrib/streaming/hadoop-streaming-1.0.3.jar -D mapred.reduce.tasks=1 -input input -output output -mapper '/Users/hhimanshu/code/p/java/hadoop-programs/hadoop-programs/src/main/python_scripts/AttributeMax.py 8' -file /Users/hhimanshu/code/p/java/hadoop-programs/hadoop-programs/src/main/python_scripts/AttributeMax.py
mapred.reduce.tasks=1 will by default means IdentityReducer so you don't need to give any reducer via -reducer option
-file option package your executable file as part of the job submission.
"""
import sys

index = int(sys.argv[1])
max = 0
for line in sys.stdin:
    fields = line.strip().split(",")
    if fields[index].isdigit():
        val = int(fields[index])
        if (val > max):
            max = val
else:
    print max
    