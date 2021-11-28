
# Table: Seat

```text
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
+-------------+---------+

```
- id is the primary key column for this table, is a continuous increment.
- Each row of this table indicates the name and the ID of a student.

 
# Question

Write an SQL query to swap the seat id of every two consecutive students. If the number of students is odd, the id of the last student is not swapped.
Return the result table ordered by id in ascending order.


# Example

The query result format is in the following example.

 
## Example 1:

Input: 
```text
Seat table:
+----+---------+
| id | student |
+----+---------+
| 1  | Abbot   |
| 2  | Doris   |
| 3  | Emerson |
| 4  | Green   |
| 5  | Jeames  |
+----+---------+
```

Output: 
```text
+----+---------+
| id | student |
+----+---------+
| 1  | Doris   |
| 2  | Abbot   |
| 3  | Green   |
| 4  | Emerson |
| 5  | Jeames  |
+----+---------+

```

# Explanation: 

Note that if the number of students is odd, there is no need to change the last one's seat.


# Sql solution

```sql
select (case
        when ((id%2)=1 and id!=(select count(*) from seat)) then id+1
        when ((id%2)=0) then id-1
        else id
        end
       ) id, 
       student
from seat
order by id
```