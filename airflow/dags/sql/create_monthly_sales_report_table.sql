-- create final table

CREATE TABLE
    IF NOT EXISTS {{var.value.get('FINAL_DATA_TABLE_NAME', 'products_monthly_sales')}} (
        transaction_date DATE NOT NULL,
        ean TEXT NOT NULL,
        price NUMERIC NOT NULL,
        quantity NUMERIC NOT NULL,
        amount NUMERIC NOT NULL,
        PRIMARY KEY (transaction_date, ean, price)
    );