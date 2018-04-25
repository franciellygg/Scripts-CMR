create table fran_28_cmr as
select A.numero_cpf_cnpj, A.id_data_hora, B.codigo_transacao, B.id_canal, B.valor, B.id_status_transacao
from cmr_15k_1 as A full join transacao_cpf_ag_conta_16_17_v1 as B 
on A.numero_cpf_cnpj = B.numero_cpf_cnpj and A.id_data_hora = B.id_data_hora;
where month(a.id_data_hora) = 9 order by numero_cpf_cnpj;


select count (distinct numero_cpf_cnpj), id_canal from fran_28_cmr group by id_canal;

select count (distinct numero_cpf_cnpj), codigo_transacao from fran_28_cmr group by codigo_transacao;

