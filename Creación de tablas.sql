-- Creación de tablas
CREATE TABLE film (
    film_id SMALLINT NOT NULL,
    title VARCHAR(255),
    description TEXT,
    release_year INT,
    language_id INT,
    original_language_id INT,
    rental_duration INT,
    rental_rate DECIMAL(4,2),
    length INT,
    replacement_cost DECIMAL(5,2),
    num_voted_users INT,
    rating VARCHAR(255),
    special_features VARCHAR(255),
    last_update TIMESTAMP,
    PRIMARY KEY (film_id)
);

CREATE TABLE store (
    store_id INT NOT NULL,
    manager_staff_id INT,
    address_id INT,
    last_update TIMESTAMP,
    PRIMARY KEY (store_id)
);

CREATE TABLE customer (
    customer_id INT NOT NULL,
    store_id INT NOT NULL REFERENCES store(store_id),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    address_id INT,
    active BOOLEAN,
    create_date TIMESTAMP,
    last_update TIMESTAMP,
    customer_id_old VARCHAR(255),
    segment VARCHAR(255),
    PRIMARY KEY (customer_id)
);

CREATE TABLE inventory (
    inventory_id INT NOT NULL,
    film_id INT NOT NULL REFERENCES film(film_id),
    store_id INT REFERENCES store(store_id),
    last_update TIMESTAMP,
    PRIMARY KEY (inventory_id)
);

CREATE TABLE rental (
    rental_id INT NOT NULL,
    rental_date TIMESTAMP,
    inventory_id INT NOT NULL REFERENCES inventory(inventory_id),
    customer_id INT NOT NULL REFERENCES customer(customer_id),
    return_date TIMESTAMP,
    staff_id INT,
    last_update TIMESTAMP,
    PRIMARY KEY (rental_id)
);





