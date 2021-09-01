The team has open questions about using Spark.
1) What's the process for performance tuning? E.g., driver memory versus executor memory, determining the number of cores per executor, determining the number of nodes.
2) What's the garbage collection frequency and are there ways to control this?
3) Python serialization / deserialization
4) What are the best practices for CPU and IO as they relate to NumPy, Spark, and Python? I'm not sure what this note was about.
5) Are there libraries for handling hierarchical workloads. Can we make custom partitioners? Will Spark maintain partitions if processes are nested?
6) How can we control recomputations?

We had additional training ideas, not necessarily related to spark.
1) Mixed integer linear programming
2) Convex programming
3) Documentation systems in python? Can we use pydoc or something else to use the markdown in source code to create documentation?
4) Dask for Python.
