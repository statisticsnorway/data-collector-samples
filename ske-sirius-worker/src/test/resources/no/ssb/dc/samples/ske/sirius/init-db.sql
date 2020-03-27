DROP TABLE IF EXISTS "TAX_RETURN";
DROP TABLE IF EXISTS "IDENTITY_MAP";
DROP TABLE IF EXISTS "TAX_RETURN_STREAM";

CREATE TABLE "TAX_RETURN"
(
    ulid            uuid                     NOT NULL, /* unique sortable id */
    position        varchar                  NOT NULL, /* hendelseliste sekvensnummer */
    income_year     int                      NOT NULL, /* hendelseliste income year */
    reg_date        timestamp with time zone NOT NULL, /* hendelseliste juridical registration date */
    event_type      varchar                  NOT NULL, /* hendelseliste hendelsetype */
    feed_identifier varchar                  NOT NULL, /* hendelseliste  identifier */
    identifier      varchar                  NULL, /* skattemelding identifier */
    shielded        bit                      NULL, /* skattemelding skjermet */
    status_code     int                      NOT NULL, /* manifest status code */
    error_code      varchar                  NULL, /* skattemedling response containing feil kode */
    PRIMARY KEY (ulid),
    UNIQUE (feed_identifier, income_year, reg_date)
);

CREATE TABLE "IDENTITY_MAP"
(
    fid         varchar                  NOT NULL,
    identity    varchar                  NOT NULL,
    income_year int                      NOT NULL,
    reg_date    timestamp with time zone NOT NULL,
    event_type  varchar                  NOT NULL,
    PRIMARY KEY (identity, income_year, reg_date)
);

CREATE TABLE "TAX_RETURN_STREAM"
(
    ulid        uuid                     NOT NULL, /* unique sortable id */
    position    varchar                  NOT NULL, /* hendelseliste sekvensnummer */
    identifier  varchar                  NULL, /* skattemelding identifier */
    income_year int                      NOT NULL, /* hendelseliste income year */
    reg_date    timestamp with time zone NOT NULL, /* hendelseliste juridical registration date */
    shielded    bit                      NULL, /* skattemelding skjermet */
    PRIMARY KEY (ulid),
    UNIQUE (identifier, income_year)
);
