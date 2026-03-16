
DROP TABLE IF EXISTS hr_final_projects.team_6_corp_loans_debt CASCADE;
CREATE TABLE hr_final_projects.team_6_corp_loans_debt (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2019 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2020 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2021 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2022 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2023 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2024 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2025 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_2026 PARTITION OF hr_final_projects.team_6_corp_loans_debt FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_debt_default PARTITION OF hr_final_projects.team_6_corp_loans_debt DEFAULT;

CREATE INDEX idx_corp_loans_debt_date        ON hr_final_projects.team_6_corp_loans_debt (date);
CREATE INDEX idx_corp_loans_debt_region      ON hr_final_projects.team_6_corp_loans_debt (measure_name);
CREATE INDEX idx_corp_loans_debt_date_region ON hr_final_projects.team_6_corp_loans_debt (date, measure_name);
CREATE INDEX idx_corp_loans_debt_element     ON hr_final_projects.team_6_corp_loans_debt (element_id);
CREATE INDEX idx_corp_loans_debt_measure     ON hr_final_projects.team_6_corp_loans_debt (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_corp_loans_overdue CASCADE;
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2019 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2020 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2021 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2022 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2023 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2024 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2025 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_2026 PARTITION OF hr_final_projects.team_6_corp_loans_overdue FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_overdue_default PARTITION OF hr_final_projects.team_6_corp_loans_overdue DEFAULT;

CREATE INDEX idx_corp_loans_overdue_date        ON hr_final_projects.team_6_corp_loans_overdue (date);
CREATE INDEX idx_corp_loans_overdue_region      ON hr_final_projects.team_6_corp_loans_overdue (measure_name);
CREATE INDEX idx_corp_loans_overdue_date_region ON hr_final_projects.team_6_corp_loans_overdue (date, measure_name);
CREATE INDEX idx_corp_loans_overdue_element     ON hr_final_projects.team_6_corp_loans_overdue (element_id);
CREATE INDEX idx_corp_loans_overdue_measure     ON hr_final_projects.team_6_corp_loans_overdue (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_corp_loans_volume CASCADE;
CREATE TABLE hr_final_projects.team_6_corp_loans_volume (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2019 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2020 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2021 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2022 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2023 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2024 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2025 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_2026 PARTITION OF hr_final_projects.team_6_corp_loans_volume FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_corp_loans_volume_default PARTITION OF hr_final_projects.team_6_corp_loans_volume DEFAULT;

CREATE INDEX idx_corp_loans_volume_date        ON hr_final_projects.team_6_corp_loans_volume (date);
CREATE INDEX idx_corp_loans_volume_region      ON hr_final_projects.team_6_corp_loans_volume (measure_name);
CREATE INDEX idx_corp_loans_volume_date_region ON hr_final_projects.team_6_corp_loans_volume (date, measure_name);
CREATE INDEX idx_corp_loans_volume_element     ON hr_final_projects.team_6_corp_loans_volume (element_id);
CREATE INDEX idx_corp_loans_volume_measure     ON hr_final_projects.team_6_corp_loans_volume (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_corp_sme_loans CASCADE;
CREATE TABLE hr_final_projects.team_6_corp_sme_loans (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2019 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2020 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2021 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2022 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2023 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2024 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2025 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_2026 PARTITION OF hr_final_projects.team_6_corp_sme_loans FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_corp_sme_loans_default PARTITION OF hr_final_projects.team_6_corp_sme_loans DEFAULT;

CREATE INDEX idx_corp_sme_loans_date        ON hr_final_projects.team_6_corp_sme_loans (date);
CREATE INDEX idx_corp_sme_loans_region      ON hr_final_projects.team_6_corp_sme_loans (measure_name);
CREATE INDEX idx_corp_sme_loans_date_region ON hr_final_projects.team_6_corp_sme_loans (date, measure_name);
CREATE INDEX idx_corp_sme_loans_element     ON hr_final_projects.team_6_corp_sme_loans (element_id);
CREATE INDEX idx_corp_sme_loans_measure     ON hr_final_projects.team_6_corp_sme_loans (measure_id);


DROP TABLE IF EXISTS hr_final_projects.team_6_deposits CASCADE;
CREATE TABLE hr_final_projects.team_6_deposits (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_deposits_2019 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2020 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2021 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2022 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2023 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2024 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2025 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_2026 PARTITION OF hr_final_projects.team_6_deposits FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_deposits_default PARTITION OF hr_final_projects.team_6_deposits DEFAULT;

CREATE INDEX idx_deposits_date        ON hr_final_projects.team_6_deposits (date);
CREATE INDEX idx_deposits_region      ON hr_final_projects.team_6_deposits (measure_name);
CREATE INDEX idx_deposits_date_region ON hr_final_projects.team_6_deposits (date, measure_name);
CREATE INDEX idx_deposits_element     ON hr_final_projects.team_6_deposits (element_id);
CREATE INDEX idx_deposits_measure     ON hr_final_projects.team_6_deposits (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_mortgage CASCADE;
CREATE TABLE hr_final_projects.team_6_mortgage (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_mortgage_2019 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2020 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2021 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2022 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2023 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2024 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2025 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_2026 PARTITION OF hr_final_projects.team_6_mortgage FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_default PARTITION OF hr_final_projects.team_6_mortgage DEFAULT;

CREATE INDEX idx_mortgage_date        ON hr_final_projects.team_6_mortgage (date);
CREATE INDEX idx_mortgage_region      ON hr_final_projects.team_6_mortgage (measure_name);
CREATE INDEX idx_mortgage_date_region ON hr_final_projects.team_6_mortgage (date, measure_name);
CREATE INDEX idx_mortgage_element     ON hr_final_projects.team_6_mortgage (element_id);
CREATE INDEX idx_mortgage_measure     ON hr_final_projects.team_6_mortgage (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_mortgage_count CASCADE;
CREATE TABLE hr_final_projects.team_6_mortgage_count (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_mortgage_count_2019 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2020 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2021 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2022 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2023 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2024 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2025 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_2026 PARTITION OF hr_final_projects.team_6_mortgage_count FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_count_default PARTITION OF hr_final_projects.team_6_mortgage_count DEFAULT;

CREATE INDEX idx_mortgage_count_date        ON hr_final_projects.team_6_mortgage_count (date);
CREATE INDEX idx_mortgage_count_region      ON hr_final_projects.team_6_mortgage_count (measure_name);
CREATE INDEX idx_mortgage_count_date_region ON hr_final_projects.team_6_mortgage_count (date, measure_name);
CREATE INDEX idx_mortgage_count_element     ON hr_final_projects.team_6_mortgage_count (element_id);
CREATE INDEX idx_mortgage_count_measure     ON hr_final_projects.team_6_mortgage_count (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_mortgage_debt CASCADE;
CREATE TABLE hr_final_projects.team_6_mortgage_debt (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_mortgage_debt_2019 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2020 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2021 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2022 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2023 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2024 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2025 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_2026 PARTITION OF hr_final_projects.team_6_mortgage_debt FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_debt_default PARTITION OF hr_final_projects.team_6_mortgage_debt DEFAULT;

CREATE INDEX idx_mortgage_debt_date        ON hr_final_projects.team_6_mortgage_debt (date);
CREATE INDEX idx_mortgage_debt_region      ON hr_final_projects.team_6_mortgage_debt (measure_name);
CREATE INDEX idx_mortgage_debt_date_region ON hr_final_projects.team_6_mortgage_debt (date, measure_name);
CREATE INDEX idx_mortgage_debt_element     ON hr_final_projects.team_6_mortgage_debt (element_id);
CREATE INDEX idx_mortgage_debt_measure     ON hr_final_projects.team_6_mortgage_debt (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_mortgage_overdue CASCADE;
CREATE TABLE hr_final_projects.team_6_mortgage_overdue (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2019 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2020 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2021 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2022 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2023 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2024 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2025 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_2026 PARTITION OF hr_final_projects.team_6_mortgage_overdue FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_overdue_default PARTITION OF hr_final_projects.team_6_mortgage_overdue DEFAULT;

CREATE INDEX idx_mortgage_overdue_date        ON hr_final_projects.team_6_mortgage_overdue (date);
CREATE INDEX idx_mortgage_overdue_region      ON hr_final_projects.team_6_mortgage_overdue (measure_name);
CREATE INDEX idx_mortgage_overdue_date_region ON hr_final_projects.team_6_mortgage_overdue (date, measure_name);
CREATE INDEX idx_mortgage_overdue_element     ON hr_final_projects.team_6_mortgage_overdue (element_id);
CREATE INDEX idx_mortgage_overdue_measure     ON hr_final_projects.team_6_mortgage_overdue (measure_id);


DROP TABLE IF EXISTS hr_final_projects.team_6_mortgage_term CASCADE;
CREATE TABLE hr_final_projects.team_6_mortgage_term (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_mortgage_term_2019 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2020 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2021 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2022 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2023 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2024 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2025 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_2026 PARTITION OF hr_final_projects.team_6_mortgage_term FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_term_default PARTITION OF hr_final_projects.team_6_mortgage_term DEFAULT;

CREATE INDEX idx_mortgage_term_date        ON hr_final_projects.team_6_mortgage_term (date);
CREATE INDEX idx_mortgage_term_region      ON hr_final_projects.team_6_mortgage_term (measure_name);
CREATE INDEX idx_mortgage_term_date_region ON hr_final_projects.team_6_mortgage_term (date, measure_name);
CREATE INDEX idx_mortgage_term_element     ON hr_final_projects.team_6_mortgage_term (element_id);
CREATE INDEX idx_mortgage_term_measure     ON hr_final_projects.team_6_mortgage_term (measure_id);


DROP TABLE IF EXISTS hr_final_projects.team_6_mortgage_volume CASCADE;
CREATE TABLE hr_final_projects.team_6_mortgage_volume (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_mortgage_volume_2019 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2020 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2021 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2022 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2023 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2024 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2025 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_2026 PARTITION OF hr_final_projects.team_6_mortgage_volume FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_mortgage_volume_default PARTITION OF hr_final_projects.team_6_mortgage_volume DEFAULT;

CREATE INDEX idx_mortgage_volume_date        ON hr_final_projects.team_6_mortgage_volume (date);
CREATE INDEX idx_mortgage_volume_region      ON hr_final_projects.team_6_mortgage_volume (measure_name);
CREATE INDEX idx_mortgage_volume_date_region ON hr_final_projects.team_6_mortgage_volume (date, measure_name);
CREATE INDEX idx_mortgage_volume_element     ON hr_final_projects.team_6_mortgage_volume (element_id);
CREATE INDEX idx_mortgage_volume_measure     ON hr_final_projects.team_6_mortgage_volume (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_retail_loans CASCADE;
CREATE TABLE hr_final_projects.team_6_retail_loans (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_retail_loans_2019 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2020 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2021 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2022 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2023 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2024 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2025 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_2026 PARTITION OF hr_final_projects.team_6_retail_loans FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_default PARTITION OF hr_final_projects.team_6_retail_loans DEFAULT;

CREATE INDEX idx_retail_loans_date        ON hr_final_projects.team_6_retail_loans (date);
CREATE INDEX idx_retail_loans_region      ON hr_final_projects.team_6_retail_loans (measure_name);
CREATE INDEX idx_retail_loans_date_region ON hr_final_projects.team_6_retail_loans (date, measure_name);
CREATE INDEX idx_retail_loans_element     ON hr_final_projects.team_6_retail_loans (element_id);
CREATE INDEX idx_retail_loans_measure     ON hr_final_projects.team_6_retail_loans (measure_id);


DROP TABLE IF EXISTS hr_final_projects.team_6_retail_loans_debt CASCADE;
CREATE TABLE hr_final_projects.team_6_retail_loans_debt (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2019 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2020 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2021 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2022 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2023 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2024 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2025 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_2026 PARTITION OF hr_final_projects.team_6_retail_loans_debt FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_debt_default PARTITION OF hr_final_projects.team_6_retail_loans_debt DEFAULT;

CREATE INDEX idx_retail_loans_debt_date        ON hr_final_projects.team_6_retail_loans_debt (date);
CREATE INDEX idx_retail_loans_debt_region      ON hr_final_projects.team_6_retail_loans_debt (measure_name);
CREATE INDEX idx_retail_loans_debt_date_region ON hr_final_projects.team_6_retail_loans_debt (date, measure_name);
CREATE INDEX idx_retail_loans_debt_element     ON hr_final_projects.team_6_retail_loans_debt (element_id);
CREATE INDEX idx_retail_loans_debt_measure     ON hr_final_projects.team_6_retail_loans_debt (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_retail_loans_overdue CASCADE;
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2019 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2020 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2021 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2022 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2023 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2024 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2025 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_2026 PARTITION OF hr_final_projects.team_6_retail_loans_overdue FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_overdue_default PARTITION OF hr_final_projects.team_6_retail_loans_overdue DEFAULT;

CREATE INDEX idx_retail_loans_overdue_date        ON hr_final_projects.team_6_retail_loans_overdue (date);
CREATE INDEX idx_retail_loans_overdue_region      ON hr_final_projects.team_6_retail_loans_overdue (measure_name);
CREATE INDEX idx_retail_loans_overdue_date_region ON hr_final_projects.team_6_retail_loans_overdue (date, measure_name);
CREATE INDEX idx_retail_loans_overdue_element     ON hr_final_projects.team_6_retail_loans_overdue (element_id);
CREATE INDEX idx_retail_loans_overdue_measure     ON hr_final_projects.team_6_retail_loans_overdue (measure_id);


DROP TABLE IF EXISTS hr_final_projects.team_6_retail_loans_volume CASCADE;
CREATE TABLE hr_final_projects.team_6_retail_loans_volume (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2019 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2020 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2021 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2022 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2023 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2024 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2025 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_2026 PARTITION OF hr_final_projects.team_6_retail_loans_volume FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_retail_loans_volume_default PARTITION OF hr_final_projects.team_6_retail_loans_volume DEFAULT;

CREATE INDEX idx_retail_loans_volume_date        ON hr_final_projects.team_6_retail_loans_volume (date);
CREATE INDEX idx_retail_loans_volume_region      ON hr_final_projects.team_6_retail_loans_volume (measure_name);
CREATE INDEX idx_retail_loans_volume_date_region ON hr_final_projects.team_6_retail_loans_volume (date, measure_name);
CREATE INDEX idx_retail_loans_volume_element     ON hr_final_projects.team_6_retail_loans_volume (element_id);
CREATE INDEX idx_retail_loans_volume_measure     ON hr_final_projects.team_6_retail_loans_volume (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_sme_loans_debt CASCADE;
CREATE TABLE hr_final_projects.team_6_sme_loans_debt (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2019 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2020 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2021 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2022 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2023 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2024 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2025 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_2026 PARTITION OF hr_final_projects.team_6_sme_loans_debt FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_debt_default PARTITION OF hr_final_projects.team_6_sme_loans_debt DEFAULT;

CREATE INDEX idx_sme_loans_debt_date        ON hr_final_projects.team_6_sme_loans_debt (date);
CREATE INDEX idx_sme_loans_debt_region      ON hr_final_projects.team_6_sme_loans_debt (measure_name);
CREATE INDEX idx_sme_loans_debt_date_region ON hr_final_projects.team_6_sme_loans_debt (date, measure_name);
CREATE INDEX idx_sme_loans_debt_element     ON hr_final_projects.team_6_sme_loans_debt (element_id);
CREATE INDEX idx_sme_loans_debt_measure     ON hr_final_projects.team_6_sme_loans_debt (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_sme_loans_overdue CASCADE;
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2019 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2020 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2021 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2022 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2023 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2024 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2025 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_2026 PARTITION OF hr_final_projects.team_6_sme_loans_overdue FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_overdue_default PARTITION OF hr_final_projects.team_6_sme_loans_overdue DEFAULT;

CREATE INDEX idx_sme_loans_overdue_date        ON hr_final_projects.team_6_sme_loans_overdue (date);
CREATE INDEX idx_sme_loans_overdue_region      ON hr_final_projects.team_6_sme_loans_overdue (measure_name);
CREATE INDEX idx_sme_loans_overdue_date_region ON hr_final_projects.team_6_sme_loans_overdue (date, measure_name);
CREATE INDEX idx_sme_loans_overdue_element     ON hr_final_projects.team_6_sme_loans_overdue (element_id);
CREATE INDEX idx_sme_loans_overdue_measure     ON hr_final_projects.team_6_sme_loans_overdue (measure_id);



DROP TABLE IF EXISTS hr_final_projects.team_6_sme_loans_volume CASCADE;
CREATE TABLE hr_final_projects.team_6_sme_loans_volume (
    col_id              INTEGER,
    element_id          INTEGER,
    measure_id          INTEGER,
    unit_id             INTEGER,
    obs_val             NUMERIC(20, 6),
    row_id              INTEGER,
    dt                  VARCHAR(50),
    periodicity         VARCHAR(20),
    date                DATE            NOT NULL,
    digits              SMALLINT,
    element_name        VARCHAR(512),
    unit_name           VARCHAR(100),
    product_key         VARCHAR(100),
    product_description VARCHAR(512),
    measure_name        VARCHAR(512)    NOT NULL
) PARTITION BY RANGE (date);

CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2019 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2020 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2021 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2022 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2023 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2024 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2025 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_2026 PARTITION OF hr_final_projects.team_6_sme_loans_volume FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE hr_final_projects.team_6_sme_loans_volume_default PARTITION OF hr_final_projects.team_6_sme_loans_volume DEFAULT;

CREATE INDEX idx_sme_loans_volume_date        ON hr_final_projects.team_6_sme_loans_volume (date);
CREATE INDEX idx_sme_loans_volume_region      ON hr_final_projects.team_6_sme_loans_volume (measure_name);
CREATE INDEX idx_sme_loans_volume_date_region ON hr_final_projects.team_6_sme_loans_volume (date, measure_name);
CREATE INDEX idx_sme_loans_volume_element     ON hr_final_projects.team_6_sme_loans_volume (element_id);
CREATE INDEX idx_sme_loans_volume_measure     ON hr_final_projects.team_6_sme_loans_volume (measure_id);