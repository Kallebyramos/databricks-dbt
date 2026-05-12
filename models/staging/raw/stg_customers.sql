-- stg_customers.sql
-- Cleans and standardizes raw customer data.
-- Known issues resolved: inconsistent country names, document types, date formats,
-- name casing, extra whitespace, and status values in mixed pt/en.

with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

cleaned as (

    select
        id                                              as customer_id,
        trim(lower(nome))                               as first_name,
        trim(lower(sobrenome))                          as last_name,
        upper(trim(documento_tipo))                     as identifier_type,
        documento_numero                                as identifier_value_raw, -- hashed downstream, never exposed in marts
        lower(trim(email))                              as email,
        telefone                                        as phone_raw,
        endereco                                        as address_raw,
        trim(cidade)                                    as city_name,
        trim(estado)                                    as state_code,

        -- Country: raw has Brasil, BR, brazil, Mexico, México, MX, mexico, Colombia, CO, colombia
        case
            when lower(trim(pais)) in ('brasil', 'br', 'brazil')    then 'BR'
            when lower(trim(pais)) in ('mexico', 'méxico', 'mx')    then 'MX'
            when lower(trim(pais)) in ('colombia', 'co')            then 'CO'
            else upper(trim(pais))
        end                                             as country_code,

        -- Currency: raw has BRL, brl, R$, reais, MXN, mxn, Pesos, COP, cop, pesos_co, empty
        case
            when lower(trim(moeda_preferida)) in ('brl', 'r$', 'reais')       then 'BRL'
            when lower(trim(moeda_preferida)) in ('mxn', 'mx$', 'pesos')      then 'MXN'
            when lower(trim(moeda_preferida)) in ('cop', 'pesos_co')           then 'COP'
            when trim(moeda_preferida) = ''                                     then null
            else upper(trim(moeda_preferida))
        end                                             as preferred_currency_code,

        -- Status: raw has ativo, ATIVO, active, Ativo, inativo, INATIVO, bloqueado, suspended
        case
            when lower(trim(status)) in ('ativo', 'active', 'ativa')       then 'active'
            when lower(trim(status)) in ('inativo', 'inactive')            then 'inactive'
            when lower(trim(status)) in ('bloqueado', 'suspended')         then 'suspended'
            else lower(trim(status))
        end                                             as status,

        -- Date: raw has YYYY-MM-DD HH:MM:SS, DD/MM/YYYY HH:MM, YYYY-MM-DDTHH:MM:SSZ
        -- TODO: implement parse_brazilian_date macro for proper handling
        data_cadastro                                   as created_at_raw

    from source

)

select * from cleaned
