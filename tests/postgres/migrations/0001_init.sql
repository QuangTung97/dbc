CREATE TABLE auth_user
(
    id         SERIAL PRIMARY KEY,
    username   VARCHAR(255) NOT NULL,
    age        INT          NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_permission
(
    user_id    INT          NOT NULL,
    perm       VARCHAR(255) NOT NULL,
    perm_desc  VARCHAR(512) NULL,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (user_id, perm)
);
