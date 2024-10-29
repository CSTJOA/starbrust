-- External csv data from s3
select * from hive.ext_data.ecs_cpu;
select * from hive.ext_data.ecs_memory;
select * from hive.ext_data.covid_19
select * from hive.ext_data.live_cholera
select * from hive.ext_data.country_location

-- Data in PostgreSQL and MySQL
select * from "postgres-ext".data.covid_19;
select * from "mysql-ext".data.live_cholera;


-- Cross data source 
select covid.country
, max(covid.cumulative_cases) as case_total_covid
, max(cholera.case_total) as case_total_cholera
from "postgres-ext".data.covid_19 as covid
left outer join "mysql-ext".data.live_cholera as cholera
  on upper(covid.country) = upper(cholera.adm0_name)
group by covid.country