CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE action_type (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE target_types (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE topics (
    id SERIAL PRIMARY KEY,
    title TEXT,
    text TEXT NOT NULL,
    author INTEGER NOT NULL REFERENCES users(id),
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    text TEXT NOT NULL,
    topic INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    author INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    action INTEGER NOT NULL REFERENCES action_type(id),
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    target_type INTEGER REFERENCES target_types(id),
    target_id INTEGER,
    success BOOLEAN NOT NULL
);


INSERT INTO action_type (id, name) VALUES
(1, 'first_visit'),
(2, 'register'),
(3, 'login'),
(4, 'logout'),
(5, 'create_topic'),
(6, 'visit_topic'),
(7, 'delete_topic'),
(8, 'write_message');

INSERT INTO target_types (id, name) VALUES
(1, 'message'),
(2, 'topic');