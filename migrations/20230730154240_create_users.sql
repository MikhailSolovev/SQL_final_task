-- +goose Up
-- +goose StatementBegin
CREATE USER ivan WITH PASSWORD 'password';
GRANT planadmin TO  ivan;
CREATE USER sophie WITH PASSWORD 'password';
GRANT planmanager TO  sophie;
CREATE USER kirill WITH PASSWORD 'password';
GRANT planmanager TO  kirill;

INSERT INTO country_managers(username, country)
    VALUES ('sophie', 'US'),
           ('sophie', 'CA'),
           ('kirill', 'FR'),
           ('kirill', 'GB'),
           ('kirill', 'DE'),
           ('kirill', 'AU');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP USER kirill;
DROP USER sophie;
DROP USER ivan;

TRUNCATE TABLE country_managers;
-- +goose StatementEnd
