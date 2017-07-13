# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution

__TODO:__ Please explain your solution briefly and highlight the advantages and disadvantages of your implementation.

### Example

#### Input

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN: 978-1449358624 |

#### Output

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN: 978-1449358624 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-EAN: 978 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-GROUP: 14 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-PUBLISHER: 4935 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-TITLE: 862 |