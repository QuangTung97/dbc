CREATE TABLE auth_user (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT NOT NULL,
    age INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE user_permission (
    user_id INTEGER NOT NULL,
    perm TEXT NOT NULL,
    perm_desc TEXT NULL,
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);
