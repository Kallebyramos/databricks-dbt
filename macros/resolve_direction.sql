{% macro resolve_direction(direction_field, amount_field) %}
{#
    Resolves transaction direction from the messy raw data.
    raw_transactions encodes direction three different ways:
      1. In a dedicated field (IN/OUT/entrada/saida/E/S/credito/debito)
      2. In the sign of the amount (negative = outgoing)
      3. Sometimes both, sometimes neither

    Returns 'IN' or 'OUT'.

    Usage: {{ resolve_direction('direcao', 'valor') }}
#}
    case
        when lower({{ direction_field }}) in ('in', 'entrada', 'e', 'credito')  then 'IN'
        when lower({{ direction_field }}) in ('out', 'saida', 's', 'debito')    then 'OUT'
        when {{ direction_field }} is null or trim({{ direction_field }}) = '' then
            case
                when cast({{ amount_field }} as decimal(18,2)) < 0 then 'OUT'
                else 'IN'
            end
        else 'IN'
    end
{% endmacro %}
