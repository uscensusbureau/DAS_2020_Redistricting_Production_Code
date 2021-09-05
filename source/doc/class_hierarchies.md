programs/optimizers:

```
Optimizer (generic Gurobi-dependent optimizers; optimizer.py)
|
|
|____ Cut-plane optimizer? (not implemented)
|
|
|____ Fourier-Motzkin elimination optimizer? (not implemented)
|
|
|____ GeoOptimizer (optimizers mapping node-to-node or node-to-child node(s) solutions; optimizer.py)
      |
      |
      |_____ l2GeoOpt (standard float-valued L2 solve, w/ nnls support; geo_optimizers.py)
      |
      |
      |_____ geoRound (standard integer-valued L1 'rounding' solve; geo_optimizers.py)
      
SequentialOptimizer (complex sequence of Optimizers, chains 1-or-more Optimizers together; optimizer.py)
|
|
|___ L2PlusRounderWithBackup (l2GeoOpt + geoRound w/ degree-of-infeasibility failsafe; sequential_optimizers.py)
      
```
