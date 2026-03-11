CREATE TABLE auth_user
(
    id         SERIAL PRIMARY KEY,
    username   VARCHAR(255) NOT NULL,
    age        BIGINT       NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_auth_user_username UNIQUE (username)
);

CREATE INDEX idx_auth_user_age_username ON auth_user (age, username);

CREATE TABLE user_permission
(
    user_id    INT          NOT NULL,
    perm       VARCHAR(255) NOT NULL,
    perm_desc  TEXT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (user_id, perm)
);
