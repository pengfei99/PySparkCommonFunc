# Problem

Table: Department
```text
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| revenue     | int     |
| month       | varchar |
+-------------+---------+

```
(id, month) is the primary key of this table.
The table has information about the revenue of each department per month.
The month has values in 
["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].


Write an SQL query to reformat the table such that there is a department id column and a revenue column for each month.

Return the result table in any order.

The query result format is in the following example.


# Example

Example 1:
```text
Input:
Department table:
+------+---------+-------+
| id   | revenue | month |
+------+---------+-------+
| 1    | 8000    | Jan   |
| 2    | 9000    | Jan   |
| 3    | 10000   | Feb   |
| 1    | 7000    | Feb   |
| 1    | 6000    | Mar   |
+------+---------+-------+
Output:
+------+-------------+-------------+-------------+-----+-------------+
| id   | Jan_Revenue | Feb_Revenue | Mar_Revenue | ... | Dec_Revenue |
+------+-------------+-------------+-------------+-----+-------------+
| 1    | 8000        | 7000        | 6000        | ... | null        |
| 2    | 9000        | null        | null        | ... | null        |
| 3    | null        | 10000       | null        | ... | null        |
+------+-------------+-------------+-------------+-----+-------------+

```
Explanation: The revenue from Apr to Dec is null.

Note that the result table has 13 columns (1 for the department id + 12 for the months).


# Solution

```sql
SELECT id,
MAX(IF(month = 'Jan', revenue, NULL)) as Jan_Revenue,
MAX(IF(month = 'Feb', revenue, NULL)) as Feb_Revenue,
MAX(IF(month = 'Mar', revenue, NULL)) as Mar_Revenue,
MAX(IF(month = 'Apr', revenue, NULL)) as Apr_Revenue,
MAX(IF(month = 'May', revenue, NULL)) as May_Revenue,
MAX(IF(month = 'Jun', revenue, NULL)) as Jun_Revenue,
MAX(IF(month = 'Jul', revenue, NULL)) as Jul_Revenue,
MAX(IF(month = 'Aug', revenue, NULL)) as Aug_Revenue,
MAX(IF(month = 'Sep', revenue, NULL)) as Sep_Revenue,
MAX(IF(month = 'Oct', revenue, NULL)) as Oct_Revenue,
MAX(IF(month = 'Nov', revenue, NULL)) as Nov_Revenue,
MAX(IF(month = 'Dec', revenue, NULL)) as Dec_Revenue
FROM Department
group by id
```