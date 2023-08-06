-- +goose Up
-- +goose StatementBegin
GRANT USAGE ON SCHEMA public TO planadmin;
GRANT USAGE ON SCHEMA public TO planmanager;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO planadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO planmanager;

GRANT ALL ON plan_data TO planadmin;
GRANT ALL ON plan_data TO planmanager;

GRANT ALL ON plan_status TO planadmin;
GRANT SELECT, UPDATE ON plan_status TO planmanager;

GRANT ALL ON country_managers TO planadmin;
GRANT SELECT ON country_managers TO planmanager;

GRANT SELECT ON v_plan_edit TO planadmin;
GRANT SELECT, UPDATE ON v_plan_edit TO planmanager;

GRANT SELECT ON v_plan TO planadmin;
GRANT SELECT ON v_plan TO planmanager;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE SELECT ON v_plan FROM planmanager;
REVOKE SELECT ON v_plan FROM planadmin;

REVOKE SELECT, UPDATE ON v_plan_edit FROM planmanager;
REVOKE SELECT ON v_plan_edit FROM planadmin;

REVOKE SELECT ON country_managers FROM planmanager;
REVOKE ALL ON country_managers FROM planadmin;

REVOKE SELECT, UPDATE ON plan_status FROM planmanager;
REVOKE ALL ON plan_status FROM planadmin;

REVOKE ALL ON plan_data FROM planmanager;
REVOKE ALL ON plan_data FROM planadmin;

REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM planmanager;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM planadmin;

REVOKE USAGE ON SCHEMA public FROM planmanager;
REVOKE USAGE ON SCHEMA public FROM planadmin;
-- +goose StatementEnd
