-- Penjualan bersih per tanggal; status paid dan shipped yang diakui.
SELECT
    CAST(order_ts AS DATE) AS order_date,
    SUM(
        CASE WHEN status IN ('paid', 'shipped')
             THEN quantity * unit_price * (1 - discount_pct)
             ELSE 0 END
    ) AS recognized_net_sales
FROM raw_orders
GROUP BY 1
ORDER BY 1;

-- Kategori dengan penjualan yang diakui tertinggi.
SELECT
    product_category,
    SUM(
        CASE WHEN status IN ('paid', 'shipped')
             THEN quantity * unit_price * (1 - discount_pct)
             ELSE 0 END
    ) AS recognized_net_sales
FROM raw_orders
GROUP BY 1
ORDER BY 2 DESC;

-- Jumlah pesanan dan nominal per status.
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(quantity * unit_price * (1 - discount_pct)) AS net_amount
FROM raw_orders
GROUP BY 1
ORDER BY 2 DESC;

-- Metode pembayaran paling sering digunakan.
SELECT payment_method, COUNT(*) AS order_count
FROM raw_orders
GROUP BY 1
ORDER BY 2 DESC;

