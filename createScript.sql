create schema if not exists hive.ext_data WITH (location = 's3a://s3-data-ext/');
create table if not exists hive.ext_data.ecs_memory (
    time varchar,
    host varchar,
    mem_available varchar,
    mem_available_percent varchar,
    mem_cached varchar,
    mem_free varchar,
    mem_swap_cached varchar,
    mem_swap_free varchar,
    mem_swap_total varchar,
    mem_total varchar,
    mem_used varchar,
    mem_used_percent varchar,
    vdc varchar
  )
with
  (
    external_location = 's3a://s3-data-ext/ecs_data_mem',
    format = 'csv',
    skip_header_line_count = 1
  );
create table if not exists hive.ext_data.ecs_cpu (
    time varchar,
    cpu_usage_guest varchar,
    cpu_usage_guest_nice varchar,
    cpu_usage_idle varchar,
    cpu_usage_iowait varchar,
    cpu_usage_irq varchar,
    cpu_usage_nice varchar,
    cpu_usage_softirq varchar,
    cpu_usage_steal varchar,
    cpu_usage_system varchar,
    cpu_usage_user varchar,
    host varchar,
    vdc varchar
  )
with
  (
    external_location = 's3a://s3-data-ext/ecs_data_cpu',
    format = 'csv',
    skip_header_line_count = 1
  );

create table if not exists hive.ext_data.covid_19 (
    date_reported varchar,
    country_code varchar,
    country varchar,
    who_region varchar,
    new_cases varchar,
    cumulative_cases varchar,
    new_deaths varchar,
    cumulative_deaths varchar
  )
with
  (
    external_location = 's3a://s3-data-ext/covid_19_data',
    format = 'csv',
    skip_header_line_count = 1
  );

  create table if not exists hive.ext_data.live_cholera (
    adm0_name varchar,
    who_region varchar,
    iso_3_code varchar,
    first_epiwk varchar,
    last_epiwk varchar,
    case_total varchar,
    death_total varchar
  )
with
  (
    external_location = 's3a://s3-data-ext/live_cholera_data',
    format = 'csv',
    skip_header_line_count = 1
  );
create table if not exists hive.ext_data.country_location (
    country varchar,
    latitude varchar,
    longitude varchar
  )
with
  (
    external_location = 's3a://s3-data-ext/country_location',
    format = 'csv',
    skip_header_line_count = 1
  );

-- Create schema
create schema if not exists "postgres-ext".data;
create schema if not exists "mysql-ext".data;

-- Insert data from s3 into PostgreSQL first
create table "postgres-ext".data.covid_19 as select * from hive.ext_data.covid_19;
create table "mysql-ext".data.live_cholera as select * from hive.ext_data.live_cholera;

-- Create schema
create schema IF not exists iceberg.bronze WITH (location = 's3a://bronze-zone/');
create schema IF not exists iceberg.silver WITH (location = 's3a://silver-zone/');
create schema IF not exists iceberg.gold WITH (location = 's3a://gold-zone/');

create table if not exists iceberg.bronze.covid_19 (
    date_reported varchar,
    country_code varchar,
    country varchar,
    who_region varchar,
    new_cases varchar,
    cumulative_cases varchar,
    new_deaths varchar,
    cumulative_deaths varchar
);
create table if not exists iceberg.silver.covid_19 (
    date_reported varchar,
    country_code varchar,
    country varchar,
    who_region varchar,
    new_cases varchar,
    cumulative_cases varchar,
    new_deaths varchar,
    cumulative_deaths varchar,
    Eff_dt_from date,
    Eff_dt_to date
);
create table if not exists iceberg.bronze.live_cholera (
    adm0_name varchar,
    who_region varchar,
    iso_3_code varchar,
    first_epiwk varchar,
    last_epiwk varchar,
    case_total varchar,
    death_total varchar
);
create table if not exists iceberg.silver.live_cholera (
    adm0_name varchar,
    who_region varchar,
    iso_3_code varchar,
    first_epiwk varchar,
    last_epiwk varchar,
    case_total varchar,
    death_total varchar,
    Eff_dt_from date,
    Eff_dt_to date
);

