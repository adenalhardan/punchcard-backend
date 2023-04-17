CREATE TABLE event (
    host_id VARCHAR(255),
    title VARCHAR(255),
    host_name VARCHAR(255) NOT NULL,
    fields TEXT NOT NULL,
    expiration INT UNSIGNED NOT NULL,
    PRIMARY KEY (host_id, title)
);

CREATE TABLE form (
    id VARCHAR(255),
    host_id VARCHAR(255),
    event_title VARCHAR(255),
    fields TEXT NOT NULL,
    PRIMARY KEY (id, host_id, event_title),
    FOREIGN KEY (host_id, event_title) REFERENCES punchcard.event(host_id, title)
);