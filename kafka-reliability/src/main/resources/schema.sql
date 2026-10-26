CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(100) NOT NULL PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(100) NOT NULL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--to store each event's progress separately, so that we can retry failed events without affecting others
CREATE TABLE IF NOT EXISTS event_tracking (
    event_id VARCHAR(100) NOT NULL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);