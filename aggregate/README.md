Implements Q1 from the paper.

```
SELECT G, count(*), sum(V), sum (V*V)
FROM R
GROUP BY G
```
