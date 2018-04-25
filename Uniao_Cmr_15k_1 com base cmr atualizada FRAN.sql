create table cmr_15k_temp as
select numero_cpf_cnpj, dt_celebracao, vl_financiado
from cmr_15k_1;


create table dgd_prod_cmr_201711_201712_temp_fran as
select numero_cpf_cnpj, dt_celebracao, vl_financiado
from (select cpf_dig as numero_cpf_cnpj, data_contratacao as dt_celebracao, 
cast(vl_financiado as double) as vl_financiado
from dgd_prod_cmr_201711_201712
where month(data_contratacao)=12 and day(data_contratacao)>=13) as a;


create table cmr_15k_ate_fim_dezem as
select numero_cpf_cnpj, dt_celebracao, vl_financiado
from cmr_15k_temp 
union all
select numero_cpf_cnpj, dt_celebracao, vl_financiado
from dgd_prod_cmr_201711_201712_temp_fran;
