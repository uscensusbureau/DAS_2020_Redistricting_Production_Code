# Coding standards for the DAS project

Except as noted below, DAS follows the coding standards expressed in:
* [PEP 8 -- Style Guide for Python](https://www.python.org/dev/peps/pep-0008/)  --- with the exception that our maximum line length for code is 200 characters, and not 72 as specified in the standard. Docstrings should have a maximum line length of 80 characters. 
* [Google Python Style Guide](http://google.github.io/styleguide/pyguide.html) --- with the exception that our maximum line length is 200 characters, and not 80 as specified in the standard. Docstrings should have a maximum line length of 80 characters

Our Aspirational Goals
* Formal verification
* Formally verified random number generators
* Eventual Migration to language with strong-types: Until then, use [typing hints](https://docs.python.org/3/library/typing.html) and static type checking.

## Generav rules and philosophy
* Functions should not change their input arguments unless all of the following are true:
** The entire purpose of the function is to make the change.
** The change to inputs is clearly documented in the function's docstring.
** The function name indicates that something will be changed (e.g. `extend_array(array,length)`). 
* In general, do not delete items from the middle of arrays. It is slow and difficult to debug. 
* For debugging, use a logging module rather than printf() statements. The logging module can be set to log to the console *and* to a file, and the log level can be changed at runtime. Logging with Python should*  be done using the Python [logging](https://docs.python.org/3.4/library/logging.html) facility.
* [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) (Don't Repeat Yourself)
* Software matters more than the data. 
* Write for reproducibility and repeatability
* Avoid [premature optimization](http://ubiquity.acm.org/article.cfm?id=1513451)

## Invoking Programs
* In general, all configuration information should be kept in a config file.
* The only command line option that should be *required* is the full path of the config file.

## Formatting code
* Tabs should be expanded to spaces and not be embedded in programs. See https://www.jwz.org/doc/tabs-vs-spaces.html for details.
* Python code should be indented by 4 spaces.
* We'll use **two** blank lines to separate *class definitions* and *top-level functions* (i.e. functions at indentation level zero)
* *Functions defined within classes* will be surrounded by **one** blank line
* Additional blank lines may be used to demonstrate *groupings of functions*

## Naming Conventions
* **Modules** should be named using lowercase letters. Try to keep module names brief. Use underscores if needed for readability/clarity
* **Classes** should be named using CapitalCase (e.g. ```class ClassName()```)
* **Functions** should be named using camelCase (e.g. ```def toCamelCase(some_string)```)
* **Variables** should be named using lowercase_letters_with_underscores (e.g. ```some_int = 32```)
* **Constants** should be named using ALL_CAPS_WITH_UNDERSCORES (e.g. ```NUM_OF_CORES = 16```)
* Always use the same casing for abbreviations (e.g. ```MyDASClass, DASClass, myDASFunction, dasFunction```)

## String Values in `consts.py`
* String values that need to be in multiple places and must always be exactly the same should be declared in a single python file (e.g. `consts.py`) and should be imported into the module where they are needed. This avoids typos on the string values.

## Using Python Modules
* Explain what modules are used and why.
* Place all imports after any module comments/docstrings and before module globals and constants (whenever possible)
* ***Avoid*** wildcard imports (i.e. ```from module import *```)
* Module abbreviations are allowed, but should be consistently used by all team members
    * Some standard abbreviations are `import numpy as np` and `import matplotlib.pyplot as plt`


## Type-Checking
To check the type of a variable, use `isinstance(variable_name, type)`.
Some examples:
```python
def isList(x):
   return isinstance(x, list)

if isinstance(x, dict):
   y = list(x.items())

y = isinstance(x, int)
```

## Method/Function Chaining
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

## Static Code Analysis
* test with pylint
* Migrate to code analysis with [Microsoft pyright](https://github.com/Microsoft/pyright) and [mypy](http://mypy-lang.org/)

## Dynamic Code Analysis 
* Unit tests are done with the py.test framework
* Each major function should have a unit test
* Use lots of assert() statement

