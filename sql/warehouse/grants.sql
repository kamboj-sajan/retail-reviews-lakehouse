-- What the load job may do in the warehouse, granted to the database role
-- "loader". Run by scripts/setup_warehouse.py, as the admin user, after
-- schema.sql and after it has created the role.
--
-- The load job signs in as its IAM role (rrl-glue-warehouse). That role's
-- RedshiftDbRoles tag is "loader", so Redshift gives it this role and nothing
-- more: it can replace rows in the three tables, but cannot create, alter or
-- drop anything, and cannot see any other schema.
--
-- Temporary tables (the staging tables of each load) need the TEMP privilege
-- on the database, which every user has by default.

GRANT USAGE ON SCHEMA dw TO ROLE loader;
GRANT SELECT, INSERT, DELETE ON dw.dim_date, dw.dim_product, dw.fact_review TO ROLE loader;
