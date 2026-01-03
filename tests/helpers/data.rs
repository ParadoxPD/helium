pub fn users_sql() -> &'static str {
    "
    CREATE TABLE users (
        id INT,
        name TEXT,
        age INT,
        active BOOL
    );

    INSERT INTO users VALUES (1, 'Alice', 30, true);
    INSERT INTO users VALUES (2, 'Bob', 15, false);
    "
}

pub fn orders_sql() -> &'static str {
    "
    CREATE TABLE orders (
        id INT,
        user_id INT,
        amount INT
    );

    INSERT INTO orders VALUES (1, 1, 200);
    INSERT INTO orders VALUES (2, 1, 20);
    "
}
