# Problems
Table: Logs
```text
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| num         | varchar |
+-------------+---------+

```
id is the primary key for this table.

Write an SQL query to find all numbers that appear at least three times consecutively.

Return the result table in any order.


# Example

The query result format is in the following example.

Example 1:

Input:

```text
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+

Output:
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+

```
Explanation: 1 is the only number that appears consecutively for at least three times.


# sql solution

```sql
-- solution 1, use select and where

select distinct l1.num as ConsecutiveNums
from logs l1,
     logs l2,
     logs l3
where l1.num=l2.num 
  and l2.num=l3.num
  and l1.id=l2.id-1
  and l1.id=l3.id-2

-- solution 2, use inner join

select distinct l1.num as ConsecutiveNums
from logs l1
inner join logs l2 on l1.num=l2.num and l1.id=l2.id-1
inner join logs l3 on l1.num=l3.num and l1.id=l3.id-2
```