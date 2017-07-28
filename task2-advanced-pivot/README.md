# advanced-pivot

The module pivots the machine data and generates a feature matrix. The feature matrix has a row for each test and aggregation mode.

#### Describe your solution

In my solution I have used SparkSQL  and First and Last value window functions.
Created separate Dataframes for Measures and Test.
Join the two Dataframes on part number and aggregation mode.

**Advantages:**
Leveraged SparkSQL and Dataframes

**Disadvantages:**
Could not manage Features in Array Struct type.Converted the test cases accordingly.



__TODO:__ Please explain your solution briefly and highlight the advantages and disadvantages of your implementation. Will it scale for a large number of tests?

### Output schema

You can use a [FeatureVector](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.linalg.Vector) instead of an array[struct]. If so, please update tests accordingly.

```
 |-- part: string (nullable = true)
 |-- location: string (nullable = true)
 |-- test: string (nullable = true)
 |-- testResult: string (nullable = true)
 |-- aggregation: string (nullable = true)
 |-- features: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- value: double (nullable = false)
```

### Example

#### Input

The history of part which went to locations and has measurement results.

|Part |Location    | Name        | Type           | UnixTimestamp  | Value | TestResult |
|-----|------------|-----------|----------------|-------|-------|----|
|x01  | Station 10 | Pressure | Measurement | 1499110000 | 12.2 | NA |
|x01  | Station 10 | Flow | Measurement | 1499110200 | 5.1 | NA |
|x01  | Station 20 | Visual test | Test | 1499110400 | NaN |  passed |
|x01  | Station 20 | EOL test | Test | 1499110500 | NaN |  failed |
|x01  | Station 10 | Pressure | Measurement | 1499120000 | 12.4 | NA |
|x01  | Station 10 | Pressure | Measurement | 1499120202 | 5.15 | NA |
|x01  | Station 20 | Visual test | Test | 1499120707 | NaN |  passed |
|x01  | Station 20 | EOL test | Test | 1499120809 | NaN |  passed |

#### Output

Feature matrix for each test and different aggregation methods.

|Part |Location    | Test        | TestResult           | Aggregation  | Features |
|-----|------------|-----------|----------------|-------|-------|
|x01  | Station 20 | Visual test | passed | first | [[Pressure, 12.2], [Flow, 5.1]] |
|x01  | Station 20 | EOL test | failed | first | [[Pressure, 12.2], [Flow, 5.1]] |
|x01  | Station 20 | Visual test | passed | last | [[Pressure, 12.4], [Flow, 5.15]] |
|x01  | Station 20 | EOL test | failed | last | [[Pressure, 12.4], [Flow, 5.15]] |
