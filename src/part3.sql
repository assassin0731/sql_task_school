CREATE ROLE administrator WITH SUPERUSER CREATEDB;
ALTER ROLE administrator LOGIN;

-- Предоставление прав на редактирование и просмотр данных
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO administrator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO administrator;

CREATE ROLE visitor WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
ALTER ROLE visitor LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;


-- delete
REASSIGN OWNED BY administrator TO postgres;
DROP OWNED BY administrator;
DROP ROLE administrator;

REASSIGN OWNED BY visitor TO postgres;
DROP OWNED BY visitor;
DROP ROLE visitor;

-- psql -h localhost -p5431 -d sql2 -U visitor  