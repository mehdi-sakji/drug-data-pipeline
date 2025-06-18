SELECT
    date,
    SUM(prod_qty * prod_price) AS ventes
FROM
    transactions
WHERE
    date BETWEEN DATE(2019, 1, 1) AND DATE(2019, 12, 31)
GROUP BY
    date
ORDER BY
    date