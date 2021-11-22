# The problem

Clients do simulations before they ask credit. The time when clients officially sent the credit request, we call it
funnel_date. After evaluation, if the credit is granted, the time when clients sign the contract, we call it
signature_date.

During all this time, clients can do multiple simulations. Each simulation will have a score and a date.

We want the most recent simulation score of clients before funnel_date and signature_date Note some client may not get
the credit, so signature_date column can be null

The final result should be ordered by user id, for null score, it should print -1

# Data

user_event_date.csv

```text
id,funnel_date,signature_date
1,2021-11-10T11:54:53,2021-12-10T12:54:53
2,2020-10-10T11:54:53,2020-11-10T11:54:53
3,2019-10-10T11:54:53,2019-10-13T11:54:53
4,2018-10-10T11:54:53,
5,2017-10-10T11:54:53,
```

score_date.csv

```text
id,score_date,score
1,2021-12-09T11:54:53,70
1,2021-11-10T11:50:53,80
1,2021-11-10T11:53:53,100
1,2021-12-10T12:49:53,100
2,2020-11-09T11:54:53,70
2,2020-10-10T11:50:53,80
2,2020-10-10T11:53:53,100
2,2020-11-10T11:49:53,100
3,2019-11-09T11:54:53,70
3,2019-10-10T11:50:53,80
3,2019-10-10T11:53:53,100
3,2019-11-10T12:49:53,100
4,2018-11-09T11:54:53,70
4,2018-10-10T11:50:53,80
4,2018-10-10T11:53:53,100
4,2018-11-10T12:49:53,100
5,2017-11-09T11:54:53,70
5,2017-10-10T11:50:53,80
5,2017-10-10T11:53:53,100
5,2017-11-10T12:49:53,100
```

# example

With above data, the output should looks like this

```text
+---+------------------------+---------------------------+
| id|best_score_before_funnel|best_score_before_signature|
+---+------------------------+---------------------------+
|  1|                     100|                        100|
|  2|                     100|                        100|
|  3|                     100|                        100|
|  4|                     100|                         -1|
|  5|                     100|                         -1|
+---+------------------------+---------------------------+
```

# sql

```sql

```