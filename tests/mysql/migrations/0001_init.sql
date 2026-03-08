CREATE TABLE auth_user
(
    id         INT PRIMARY KEY AUTO_INCREMENT,
    username   VARCHAR(255) NOT NULL,
    age        INT          NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT uk_username UNIQUE (username)
);

CREATE INDEX idx_age_username ON auth_user (age, username);
