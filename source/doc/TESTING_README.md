# Testing procedures

1. Make sure the file you worked on is importable from python repl shell, starting in base directory of das_decennial
2. Create unit tests in the tests/ directory. The filename should be test\_\[name-of-file-you-are-testing\].py
3. Run unit tests for the file you modify
4. Pylint your code. It should get a 10
5. Run your code on the cluster using run\_cluster.sh with config file configs/test_RI.ini to make sure cluster capabilities have not been broken. Information about how to run in the cluster is in the main README.md of das_decennial
