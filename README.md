# dateformatting

We usually met many dates user input manually, it is hard for program processing. This library used for unify it to ISO 8601 Standard format [YYYY-mm-dd](https://en.wikipedia.org/wiki/ISO_8601).

dateformatting is written by Scala and using regular expression to distinguish those formats. There were a large number of date examples in test folder.

## Configurations
Before test, working directory should be set to project root folder.

## Dependencies
scala 2.11.8, spark 2.4.0

## Comments
<table style="width:100%">
  <tr>
    <th>Input</th>
    <th>Output</th>
  </tr>
  <tr>
    <td>20121103112058</td>
    <td>P7-2012-11-03</td>
  </tr>
  <tr>
    <td>2017-06-16 12:00:00</td>
    <td>P1-2017-06-16</td>
  </tr>
  <tr>
    <td>2016.04.01</td>
    <td>P1-2016-04-01</td>
  </tr>
  <tr>
    <td>2017年4月5日</td>
    <td>P1-2017-4-5</td>
  </tr>
  <tr>
    <td>16 June 2017</td>
    <td>P2-2017-6-16</td>
  </tr>
  <tr>
    <td>22-07-2019</td>
    <td>P4-2019-07-22</td>
  </tr>
  <tr>
    <td>6. března 2015 12:00:00</td>
    <td>P4-2015-3-6</td>
  </tr>
  <tr>
    <td>27 февраля 2015</td>
    <td>P2-2015-2-27</td>
  </tr>
</table>
* PX  means which regular expression matched.
