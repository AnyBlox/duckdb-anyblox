= ClickBench string-specific queries

This is a selection of those queries from ClickBench that perform non-trivial operations on string
columns.
The format is: Q{N} (CB {K}), where N is our simple increasing naming scheme, while K is the number of the
original ClickBench query.

== Q1 (CB6)

```sql
-- Q1 (CB6)
SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
```

`SearchPhrase` is a `TEXT` column, so this requires counting distinct strings.

== Q2 (CB14)

```sql
-- Q2 (CB14)
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```

CB13 and CB15 are subsumed by this.

== Q3 (CB19)

```sql
-- Q3 (CB19)
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```

CB17 and CB18 are subsumed by this.

== Q4 (CB21)

```sql
-- Q4 (CB21)
SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
```

Counting matches of a string pattern.

== Q5 (CB24)

```sql
-- Q5 (CB24)
SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
```

String operation + sorting by other column.

== Q6 (CB29)

```sql
-- Q6 (CB29)
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
```

Regex operation plus some more processing by its result.

== Q7 (CB33)

```sql
-- Q19 (CB33)
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```

Groupping by string plus additional non-string processing.
CB31 and CB32 are subsumed by this.

== Q8 (CB34)

```sql
-- Q8 (CB34)
SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
```

Group by and select a big (on average) string.
== Used string columns stats

| **Column**           | **count non-empty** | **count distinct** | **min len (non-empty)** | **max len** | **med len (non-empty)** | **sum len** | **avg len (non-empty** |
|----------------------|---------------------|--------------------|-------------------------|-------------|-------------------------|-------------|------------------------|
| **SearchPhrase**     | 13172392            | 6019102            | 1                       | 1113        | 28.0                    | 413315834   | 31.38                  |
| **MobilePhoneModel** | 5563212             | 165                | 2                       | 27          | 4.0                     | 23280516    | 4.18                   |
| **URL**              | 99929734            | 18342018           | 2                       | 7391        | 54.0                    | 8791293596  | 87.97                  |
| **Title**            | 85087080            | 9425423            | 1                       | 1152        | 56.0                    | 5664904533  | 66.58                  |
| **Referer**          | 81032736            | 19720796           | 10                      | 2710        | 58.0                    | 6339129459  | 78.23                  |

Extracted using the following query template for each column:

```sql
SELECT * FROM 
  (SELECT COUNT(*) AS "count non-empty" FROM hits WHERE $1 <> '') JOIN 
  (SELECT 
      COUNT(DISTINCT $1) AS "count distinct",
      MIN(LENGTH($1)) AS "min len (non-empty)",
      MAX(LENGTH($1)) AS "max len",
      MEDIAN(LENGTH($1)) AS "med len (non-empty)",
      SUM(LENGTH($1)) AS "sum len",
      AVG(LENGTH($1)) AS "avg len (non-empty)"
    FROM hits WHERE $1 <> '') ON true;
```

== Skipped queries explanation.

Queries that don't touch string columns are excluded by default.

== Q2, Q3 (CB11, CB12)

```sql
-- Q2 (CB11)
SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
-- Q3 (CB12)
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
```

`MobilePhoneModel` is `TEXT`. While `<> ''` is trivial, groupping by string is not.

== Q11 (CB22)

```sql
-- Q11 (CB22)
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

This combines the Q10 pattern search with a length computation and Q7-style GROUP BY.

== Q12 (CB23)

```sql
-- Q12 (CB23)
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

Much more complicated string search, now looking for a match in a column, an anti-match in another column,
and another Q7-style GROUP BY.

== Q14, Q15 (CB26, CB27)

```sql
-- Q14 (CB26)
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;
-- Q15 (CB27)
SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;
```

Sorting by string column.
