Receber o arquivo: BASE_CMR_CONSOLIDA_DEF_201801_mskd.txt

Fazer upload do arquivo para o Juypter Notebook, depois que fazer o upload, executar esse comando no HDFS:

#criar pasta
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/CMR_FRAN/
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/CMR_FRAN/BASE_CMR_CONSOLIDA_DEF_201801_mskd
#Copia do jupyter para o hdfs
hdfs dfs -copyFromLocal /home/fgastardi/BASE_CMR_CONSOLIDA_DEF_201801_mskd.txt /user/hive/warehouse/dcd.db/CMR_FRAN/BASE_CMR_CONSOLIDA_DEF_201801_mskd/BASE_CMR_CONSOLIDA_DEF_201801_mskd.txt


create external table BASE_CMR_CONSOLIDA_DEF_201801_mskd
(numero_cpf_cnpj string, base string, remessa_janeiro string, agencia_crm string, conta_crm string, segmento_crm string, rating string, convenio string, prazo_cmr_pricing string, limite_cmr_final_def string, valor_parcela string)
row format delimited
fields terminated by '|'
location '/user/hive/warehouse/dcd.db/CMR_FRAN/BASE_CMR_CONSOLIDA_DEF_201801_mskd';

#eliminar a primeira linha da tabela 
create table BASE_CMR_CONSOLIDA_DEF_201801 as
select numero_cpf_cnpj, base, remessa_janeiro, cast(agencia_crm as int) as agencia_crm, cast(conta_crm as int) as conta_crm, segmento_crm, rating, cast(convenio as int) as convenio, cast(prazo_cmr_pricing as int) as prazo_cmr_pricing, cast(limite_cmr_final_def as int) as limite_cmr_final_def, cast(valor_parcela as int) as valor_parcela
from BASE_CMR_CONSOLIDA_DEF_201801_mskd where numero_cpf_cnpj !='CPF11';

--------------------------------------------------------------------------------------------------------------------------------

create table 7kk_interacao_com_6kk as
select A.*, B.base, B.remessa_janeiro, B.rating, B.convenio, B.prazo_cmr_pricing, B.limite_cmr_final_def, B.valor_parcela
from 6kk_full_com_dummies_12_01_2018_v11 as A join 
base_cmr_consolida_def_201801 as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj;


create table 7kk_complemento as
select A.*
from base_cmr_consolida_def_201801 as A Left join
6kk_full_com_dummies_12_01_2018_v11 as B
on A.numero_cpf_cnpj = B.numero_cpf_cnpj
where B.numero_cpf_cnpj is NULL;

