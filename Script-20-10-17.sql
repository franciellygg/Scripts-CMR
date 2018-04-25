create table transacao_2017_parquet_2(
id_data_hora timestamp,
id_canal string,
numero_agencia int,
numero_conta string,
numero_agencia_terminal int,
numero_terminal int,
id_site int,
codigo_transacao string,
id_status_transacao string,
valor float) stored as parquet;

insert overwrite table transacao_2017_parquet_2
    select
        cast(id_data_hora as timestamp),
        id_canal,
        cast(numero_agencia as int),
        cast(numero_conta as string),
        cast(numero_agencia_terminal as int),
        cast(numero_terminal as int),
        cast(id_site as int),
        cast(codigo_transacao as string),
        cast(id_status_transacao as string),
        cast(valor as float)
    from transacao_2017;

create table transacao_2016_parquet_2(
id_data_hora timestamp,
id_canal string,
numero_agencia int,
numero_conta string,
numero_agencia_terminal int,
numero_terminal int,
id_site int,
codigo_transacao string,
id_status_transacao string,
valor float) stored as parquet;

insert overwrite table transacao_2016_parquet_2
    select
        cast(id_data_hora as timestamp),
        id_canal,
        cast(numero_agencia as int),
        cast(numero_conta as string),
        cast(numero_agencia_terminal as int),
        cast(numero_terminal as int),
        cast(id_site as int),
        cast(codigo_transacao as string),
        cast(id_status_transacao as string),
        cast(valor as float)
    from transacao_2016;




#filtrar a transacao_2016 => pegar só depois de outubro
create table transacao_2016_parquet_outubro as 
select * from transacao_2016_parquet_2 where id_data_hora >='2016-10-01';

#União das tabelas 2016_2017 => Unindo as tabelas 2016_outubro + transacao2017_parquet 

CREATE TABLE transacao_2016_U_102017 AS 
SELECT  
unioned.id_data_hora,
unioned.id_canal,
unioned.numero_agencia,
unioned.numero_conta,
unioned.numero_agencia_terminal,
unioned.numero_terminal,
unioned.id_site,
unioned.codigo_transacao,
unioned.id_status_transacao,
unioned.valor
FROM( SELECT a.id_data_hora,
a.id_canal,
a.numero_agencia,
a.numero_conta,
a.numero_agencia_terminal,
a.numero_terminal,
a.id_site,
a.codigo_transacao,
a.id_status_transacao,
a.valor FROM transacao_2016_parquet_outubro as a UNION ALL 
SELECT b.id_data_hora,
b.id_canal,
b.numero_agencia,
b.numero_conta,
b.numero_agencia_terminal,
b.numero_terminal,
b.id_site,
b.codigo_transacao,
b.id_status_transacao,
b.valor FROM transacao_102017_parquet_1 as b)unioned;

#tabela 2016_2017 + conta ativa 2017
create table transacao_cpf_ag_conta_16_17 as select 
a.id_data_hora,
a.id_canal,
b.numero_cpf_cnpj,
a.numero_agencia,
a.numero_conta,
a.numero_agencia_terminal,
a.numero_terminal,
a.id_site,
a.codigo_transacao,
a.id_status_transacao,
a.valor
from transacao_2016_U_102017 as a join conta_ativa_2017_v2_cpf_agencia_conta as b on a.numero_agencia = b.numero_agencia and a.numero_conta = b.numero_conta;


#transacao_cpf_ag_conta_16_17 + cods_classes


create table transacao_cpf_ag_conta_16_17_classes as
select a.*, 
b.desc_transacao,   
b.desc_servico,     
b.status_relatorio, 
b.super_classe,     
b.sub_classe 
from transacao_cpf_ag_conta_16_17 as a join cods_classes as b on a.codigo_transacao = b.codigo_transacao;


#criar as variaveis - ano|mes|dia|semana|hora|dia_semana|fim_de_semana
create table zz_teste1 as 
select *, 
year(id_data_hora) as ano, 
month(id_data_hora) as mes, 
day(id_data_hora) as dia, 
weekofyear(id_data_hora) as semana, 
hour(id_data_hora) as hora, 
date_format(id_data_hora, 'u') as dia_semana, 
case when date_format(id_data_hora, 'u') < 6 then 0 when date_format(id_data_hora, 'u') > 5 then 1 end as fim_de_semana
from transacao_cpf_ag_conta_16_17_classes;

# faixa hora1
create table zz_teste2 as 
select *, 
case when hora between 0 and 3 then '0:00-3:59' 
when hora between 4 and 7 then '4:00-7:59' 
when hora between 8 and 11 then '8:00-11:59' 
when hora between 12 and 15 then '12:00-15:59'
when hora between 16 and 19 then '16:00-19:59' else '20:00-23:59' end as faixa_hora1
from zz_teste1; 

# faixa hora2, MADRUGADA, MANHÃ, TARDE E NOITE.
# de 0 a 5, 6 a 12, 12 a 18, 18 a 24

create table transacao_cpf_ag_conta_16_17_classes_v2 as  
select *, 
case when hora between 0 and 5 then '0:00-5:59' 
when hora between 6 and 11 then '6:00-11:59' 
when hora between 12 and 17 then '12:00-17:59' else '18:00-23:59' end as faixa_hora2 
from zz_teste2;