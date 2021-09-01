# DAS Framework Python Code Style Guide

#### Indentation
We will use **4 spaces** per *indentation level*.

#### Naming Conventions
* **Modules** should be named using lowercase letters. Try to keep module names brief. Use underscores if needed for readability/clarity
* **Classes** should be named using CapitalCase (e.g. ```class ClassName()```)
* **Functions** should be named using camelCase (e.g. ```def toCamelCase(some_string)```)
* **Variables** should be named using lowercase_letters_with_underscores (e.g. ```some_int = 32```)
* **Constants** should be named using ALL_CAPS_WITH_UNDERSCORES (e.g. ```NUM_OF_CORES = 16```)
* Always use the same casing for abbreviations (e.g. ```MyDASClass, DASClass, myDASFunction, dasFunction```)

#### String Values
* String values that need to be in multiple places and must always be exactly the same should be declared in a single python file (e.g. `consts.py`) and should be imported into the module where they are needed. This avoids typos on the string values.

#### Imports
* Place all imports after any module comments/docstrings and before module globals and constants (whenever possible)
* ***Avoid*** wildcard imports (i.e. ```from module import *```)
* Module abbreviations are allowed, but should be consistently used by all team members
    * Some standard abbreviations are `import numpy as np` and `import matplotlib.pyplot as plt`

#### Blank lines
* We'll use **two** blank lines to separate *class definitions* and *top-level functions* (i.e. functions at indentation level zero)
* *Functions defined within classes* will be surrounded by **one** blank line
* Additional blank lines may be used to demonstrate *groupings of functions*

#### Function arguments
* Argument names should be as clear and brief as possible
* Arguments should be separated by commas with a space after each comma (e.g. ```def tertiaryOperator(condition, true_value, false_value)```)
* Keyword arguments should have no white space surrounding the equals sign (e.g. ```def draw(objs_to_draw, color="blue", size=12, save=False, transformation_fxn=None)```)

#### Type-Checking
To check the type of a variable, use `isinstance(variable_name, type)`.
Some examples:
```python
def isList(x):
   return isinstance(x, list)

if isinstance(x, dict):
   y = list(x.items())

y = isinstance(x, int)
```

#### Return Statements
Only one `return` statement should exist in a function. Use local variables to handle branching logic.
Example:
```python
# Not recommended
def branchingExample(x):
   if x < 0:
      return "negative"
   else:
      return "non-negative"

# Recommended
def branchingExample(x):
   if x < 0:
      sign = "negative"
   else:
      sign = "non-negative"
   
   return sign
```

#### Method/Function Chaining
The standard is to wrap the function calls in parentheses, making the chain more readable

Some guidelines:
* Double-indent the first line of the method chain and align all dots/function calls
* Single-indent the closing brace

For example,
```Python
from operator import add
counts_of_evens_and_odds = (        
        sc.parallelize([(str(i%2), i) for i in range(11)])
          .mapValues(lambda x: 1)
          .reduceByKey(add)
          .collect()
    )
```

## Config file Standards
In order to mitigate issues related to added/missing/altered config properties, we have decided (until a better proposal comes along) to use *__personalized config files__* with the following naming convention: **master config file** + **the author's name/initials/JBID** (e.g. `test_config_moran331.ini` or `test_config_RA.ini`).
