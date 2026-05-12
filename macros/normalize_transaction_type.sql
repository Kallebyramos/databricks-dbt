{% macro normalize_transaction_type(raw_type) %}
{#
    Maps the ~30 variants of transaction type found in raw_transactions
    into the 11 canonical types defined in seed dim_transaction_type.

    Usage: {{ normalize_transaction_type('tipo') }}
#}
    case
        when lower({{ raw_type }}) in ('pix', 'pix_enviado', 'pix_recebido')             then 'pix'
        when lower({{ raw_type }}) in ('ted', 'transferencia_ted')                         then 'ted'
        when lower({{ raw_type }}) in ('doc')                                              then 'doc'
        when lower({{ raw_type }}) in ('boleto', 'pagamento_boleto')                       then 'boleto'
        when lower({{ raw_type }}) in ('spei', 'transferencia_spei')                       then 'spei'
        when lower({{ raw_type }}) in ('compra', 'purchase', 'compra_credito')             then 'compra_credito'
        when lower({{ raw_type }}) in ('compra_debito')                                    then 'compra_debito'
        when lower({{ raw_type }}) in ('saque', 'withdrawal')                              then 'saque'
        when lower({{ raw_type }}) in ('deposito', 'deposit')                              then 'deposito'
        when lower({{ raw_type }}) in ('tarifa', 'fee', 'taxa_manutencao')                 then 'tarifa'
        when lower({{ raw_type }}) in ('estorno', 'refund', 'chargeback')                  then 'estorno'
        else lower({{ raw_type }})
    end
{% endmacro %}
