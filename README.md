# dateformatting

We usually met many dates user input manually, it is hard for program processing. This library used for unify it to ISO 8601 Standard format [YYYY-mm-dd](https://en.wikipedia.org/wiki/ISO_8601).

dateformatting is written by Scala and using regular expression to distinguish those formats. There were a large number of date examples in test folder.

## Configurations
Before test, you should set working directory to project root folder.

## Dependencies
scala 2.11.8, spark 2.4.0

## Comments
input:20121103112058		output:P7-2012-11-03
input:2017-06-16 12:00:00		output:P1-2017-06-16
input:2016.04.01		output:P1-2016-04-01
input:2017年4月5日		output:P1-2017-4-5
input:16 June 2017		output:P2-2017-6-16
input:22-07-2019		output:P4-2019-07-22
input:6. března 2015 12:00:00		output:P4-2015-3-6
input:27 февраля 2015		output:P2-2015-2-27