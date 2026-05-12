-- tests/assert_no_raw_cpf_in_marts.sql
-- LGPD compliance: ensures no raw CPF (XXX.XXX.XXX-XX) appears in dim_customers.
-- If this query returns any rows, the test FAILS.

select customer_id
from {{ ref('dim_customers') }}
where
    first_name  rlike '\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}'
    or last_name rlike '\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}'
