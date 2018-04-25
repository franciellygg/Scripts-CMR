select codigo_transacao, count (*) AS qtd_transacoes, sum(valor) as total_valor, avg(valor) as media_valor, min(valor) as min_valor, max(valor) as max_valor
from transacao_cpf_ag_conta_16_17_v1
where codigo_transacao = "FF_RESG_TOTAL" And ano = 2016 or codigo_transacao = "FF_RESG_PARCIAL" And ano = 2016 or codigo_transacao = "FF_RESG_RET.TOTAL" And ano = 2016 
or codigo_transacao = "FF_RESG_PROGRAMADO" And ano = 2016 or codigo_transacao = "FF_CPRO_CONS.FGBI" And ano = 2016
group by codigo_transacao
order by codigo_transacao desc;


select codigo_transacao, count (*) AS qtd_transacoes, sum(valor) as total_valor, avg(valor) as media_valor, min(valor) as min_valor, max(valor) as max_valor
from transacao_cpf_ag_conta_16_17_v1
where codigo_transacao = "FF_RESG_TOTAL" And ano = 2017 or codigo_transacao = "FF_RESG_PARCIAL" And ano = 2017 or codigo_transacao = "FF_RESG_RET.TOTAL" And ano = 2017 
or codigo_transacao = "FF_RESG_PROGRAMADO" And ano = 2017 or codigo_transacao = "FF_CPRO_CONS.FGBI" And ano = 2017
group by codigo_transacao
order by codigo_transacao desc;
