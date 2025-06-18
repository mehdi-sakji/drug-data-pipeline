SELECT
    client_id,
    SUM(CASE WHEN product_type = 'MEUBLE' THEN prod_qty * prod_price ELSE 0 END) AS ventes_meuble,
    SUM(CASE WHEN product_type = 'DECO' THEN prod_qty * prod_price ELSE 0 END) AS ventes_deco
FROM
    transactions
LEFT JOIN
    products_nomenclature ON transactions.prod_id = products_nomenclature.product_id
WHERE
    transactions.date BETWEEN DATE(2019, 1, 1) AND DATE(2019, 12, 31)
GROUP BY
    client_id
ORDER BY
    ventes_meuble