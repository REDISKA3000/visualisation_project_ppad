DROP TABLE IF EXISTS hr_final_projects.team_6_publications;
CREATE TABLE hr_final_projects.team_6_publications (
    id            INTEGER      NOT NULL,
    parent_id     INTEGER      NOT NULL,
    category_name VARCHAR(512) NOT NULL,
    no_active     SMALLINT     NOT NULL DEFAULT 0,
 CONSTRAINT pk_team_6_publications PRIMARY KEY (id)
);

DROP TABLE IF EXISTS hr_final_projects.team_6_datasets_catalog;
CREATE TABLE hr_final_projects.team_6_datasets_catalog (
    publication_id         INTEGER      NOT NULL,
    publication_name       VARCHAR(512) NOT NULL,
    dataset_id             INTEGER      NOT NULL,
    dataset_name           VARCHAR(512) NOT NULL,
    dataset_type           SMALLINT     NOT NULL DEFAULT 1,
    publication_no_active  SMALLINT     NOT NULL DEFAULT 0,
 
    CONSTRAINT pk_team_6_datasets_catalog PRIMARY KEY (dataset_id, publication_id)
);

DROP TABLE IF EXISTS hr_final_projects.team_6_datasets_bank_shortlist;
CREATE TABLE hr_final_projects.team_6_datasets_bank_shortlist (
    publication_id         INTEGER      NOT NULL,
    publication_name       VARCHAR(512) NOT NULL,
    dataset_id             INTEGER      NOT NULL,
    dataset_name           VARCHAR(512) NOT NULL,
    dataset_type           SMALLINT     NOT NULL DEFAULT 1,
    publication_no_active  SMALLINT     NOT NULL DEFAULT 0,
 
    CONSTRAINT pk_team_6_datasets_bank_shortlist PRIMARY KEY (dataset_id, publication_id)
);

DROP TABLE IF EXISTS hr_final_projects.team_6_measures;
CREATE TABLE hr_final_projects.team_6_measures (
    dataset_id   INTEGER      NOT NULL,
    measure_id   INTEGER      NOT NULL,
    measure_name VARCHAR(512) NOT NULL,
 
    CONSTRAINT pk_team_6_measures PRIMARY KEY (dataset_id, measure_id)
);


DROP TABLE IF EXISTS hr_final_projects.team_6_years;
CREATE TABLE hr_final_projects.team_6_years (
    dataset_id  INTEGER  NOT NULL,
    measure_id  INTEGER  NOT NULL,
    from_year   SMALLINT NOT NULL,
    to_year     SMALLINT NOT NULL,
 
    CONSTRAINT pk_team_6_years PRIMARY KEY (dataset_id, measure_id)
);