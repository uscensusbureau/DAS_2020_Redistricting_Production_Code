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
* Python 3.6.

## Using Python Modules
* Explain what modules are used and why.

## Static Code Analysis
* test with pylint
* Migrate to code analysis with [Microsoft pyright](https://github.com/Microsoft/pyright) and [mypy](http://mypy-lang.org/)

## Dynamic Code Analysis 
* Unit tests are done with the py.test framework
* Each major function should have a unit test
* Use lots of assert() statement

