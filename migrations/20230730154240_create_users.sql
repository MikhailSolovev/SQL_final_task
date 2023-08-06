-- +goose Up
-- +goose StatementBegin
CREATE USER ivan WITH ROLE planadmin;
ALTER USER ivan WITH PASSWORD 'password';
CREATE USER sophie WITH ROLE planmanager;
ALTER USER sophie WITH PASSWORD 'password';
CREATE USER kirill WITH ROLE planmanager;
ALTER USER kirill WITH PASSWORD 'password';

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
