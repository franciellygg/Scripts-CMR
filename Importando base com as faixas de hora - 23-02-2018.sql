#Criar as pastas
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_madru
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_manha
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_tarde
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_noite

#Copiar do jupyter para o HDFS
hdfs dfs -copyFromLocal /home/fgastardi/df_madru.csv /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_madru/df_madru.csv
hdfs dfs -copyFromLocal /home/fgastardi/df_manha.csv /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_manha/df_manha.csv
hdfs dfs -copyFromLocal /home/fgastardi/df_tarde.csv /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_tarde/df_tarde.csv
hdfs dfs -copyFromLocal /home/fgastardi/df_noite.csv /user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_noite/df_noite.csv

#Colocar as tabelas no hive
create external table df_madru_1 (id string, numero_cpf_cnpj string, numero_agencia string, id_codigo_segmento string, id_codigo_segmento2 string, x_q4m_bh2_0_6_mb_f_s string, x_q4m_bh2_6_12_mb_f_s string, x_q4m_bh2_12_18_mb_f_s string, x_q4m_bh2_18_24_mb_f_s string, x_q4m_bh2_0_6_mb_total_s string, x_q4m_bh2_6_12_mb_total_s string, x_q4m_bh2_12_18_mb_total_s string, x_q4m_bh2_18_24_mb_total_s string, x_q4m_bh2_0_6_f_s string, x_q4m_bh2_6_12_f_s string, x_q4m_bh2_12_18_f_s string, x_q4m_bh2_18_24_f_s string, x_q4m_bh2_0_6_total_s string, x_q4m_bh2_6_12_total_s string, x_q4m_bh2_12_18_total_s string, x_q4m_bh2_18_24_total_s string, x_q6m_bh2_0_6_mb_f string, x_q6m_bh2_6_12_mb_f string, x_q6m_bh2_12_18_mb_f string, x_q6m_bh2_18_24_mb_f string, x_q6m_bh2_0_6_mb_total string, x_q6m_bh2_6_12_mb_total string, x_q6m_bh2_12_18_mb_total string, x_q6m_bh2_18_24_mb_total string, x_q6m_bh2_0_6_f string, x_q6m_bh2_6_12_f string, x_q6m_bh2_12_18_f string, x_q6m_bh2_18_24_f string, x_q6m_bh2_0_6_total string, x_q6m_bh2_6_12_total string, x_q6m_bh2_12_18_total string, x_q6m_bh2_18_24_total string, Probabilidade_rl string, maior_F_MB string, maior_total_MB string, maior_F string, maior_total string, maior_total_MB_TODOS string)
row format delimited
fields terminated by "\,"
location '/user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_madru';

create external table df_manha_1 (id string, numero_cpf_cnpj string, numero_agencia string, id_codigo_segmento string, id_codigo_segmento2 string, x_q4m_bh2_0_6_mb_f_s string, x_q4m_bh2_6_12_mb_f_s string, x_q4m_bh2_12_18_mb_f_s string, x_q4m_bh2_18_24_mb_f_s string, x_q4m_bh2_0_6_mb_total_s string, x_q4m_bh2_6_12_mb_total_s string, x_q4m_bh2_12_18_mb_total_s string, x_q4m_bh2_18_24_mb_total_s string, x_q4m_bh2_0_6_f_s string, x_q4m_bh2_6_12_f_s string, x_q4m_bh2_12_18_f_s string, x_q4m_bh2_18_24_f_s string, x_q4m_bh2_0_6_total_s string, x_q4m_bh2_6_12_total_s string, x_q4m_bh2_12_18_total_s string, x_q4m_bh2_18_24_total_s string, x_q6m_bh2_0_6_mb_f string, x_q6m_bh2_6_12_mb_f string, x_q6m_bh2_12_18_mb_f string, x_q6m_bh2_18_24_mb_f string, x_q6m_bh2_0_6_mb_total string, x_q6m_bh2_6_12_mb_total string, x_q6m_bh2_12_18_mb_total string, x_q6m_bh2_18_24_mb_total string, x_q6m_bh2_0_6_f string, x_q6m_bh2_6_12_f string, x_q6m_bh2_12_18_f string, x_q6m_bh2_18_24_f string, x_q6m_bh2_0_6_total string, x_q6m_bh2_6_12_total string, x_q6m_bh2_12_18_total string, x_q6m_bh2_18_24_total string, Probabilidade_rl string, maior_F_MB string, maior_total_MB string, maior_F string, maior_total string, maior_total_MB_TODOS string)
row format delimited
fields terminated by "\,"
location '/user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_manha';

create external table df_tarde_1 (id string, numero_cpf_cnpj string, numero_agencia string, id_codigo_segmento string, id_codigo_segmento2 string, x_q4m_bh2_0_6_mb_f_s string, x_q4m_bh2_6_12_mb_f_s string, x_q4m_bh2_12_18_mb_f_s string, x_q4m_bh2_18_24_mb_f_s string, x_q4m_bh2_0_6_mb_total_s string, x_q4m_bh2_6_12_mb_total_s string, x_q4m_bh2_12_18_mb_total_s string, x_q4m_bh2_18_24_mb_total_s string, x_q4m_bh2_0_6_f_s string, x_q4m_bh2_6_12_f_s string, x_q4m_bh2_12_18_f_s string, x_q4m_bh2_18_24_f_s string, x_q4m_bh2_0_6_total_s string, x_q4m_bh2_6_12_total_s string, x_q4m_bh2_12_18_total_s string, x_q4m_bh2_18_24_total_s string, x_q6m_bh2_0_6_mb_f string, x_q6m_bh2_6_12_mb_f string, x_q6m_bh2_12_18_mb_f string, x_q6m_bh2_18_24_mb_f string, x_q6m_bh2_0_6_mb_total string, x_q6m_bh2_6_12_mb_total string, x_q6m_bh2_12_18_mb_total string, x_q6m_bh2_18_24_mb_total string, x_q6m_bh2_0_6_f string, x_q6m_bh2_6_12_f string, x_q6m_bh2_12_18_f string, x_q6m_bh2_18_24_f string, x_q6m_bh2_0_6_total string, x_q6m_bh2_6_12_total string, x_q6m_bh2_12_18_total string, x_q6m_bh2_18_24_total string, Probabilidade_rl string, maior_F_MB string, maior_total_MB string, maior_F string, maior_total string, maior_total_MB_TODOS string)
row format delimited
fields terminated by "\,"
location '/user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_tarde';

create external table df_noite_1 (id string, numero_cpf_cnpj string, numero_agencia string, id_codigo_segmento string, id_codigo_segmento2 string, x_q4m_bh2_0_6_mb_f_s string, x_q4m_bh2_6_12_mb_f_s string, x_q4m_bh2_12_18_mb_f_s string, x_q4m_bh2_18_24_mb_f_s string, x_q4m_bh2_0_6_mb_total_s string, x_q4m_bh2_6_12_mb_total_s string, x_q4m_bh2_12_18_mb_total_s string, x_q4m_bh2_18_24_mb_total_s string, x_q4m_bh2_0_6_f_s string, x_q4m_bh2_6_12_f_s string, x_q4m_bh2_12_18_f_s string, x_q4m_bh2_18_24_f_s string, x_q4m_bh2_0_6_total_s string, x_q4m_bh2_6_12_total_s string, x_q4m_bh2_12_18_total_s string, x_q4m_bh2_18_24_total_s string, x_q6m_bh2_0_6_mb_f string, x_q6m_bh2_6_12_mb_f string, x_q6m_bh2_12_18_mb_f string, x_q6m_bh2_18_24_mb_f string, x_q6m_bh2_0_6_mb_total string, x_q6m_bh2_6_12_mb_total string, x_q6m_bh2_12_18_mb_total string, x_q6m_bh2_18_24_mb_total string, x_q6m_bh2_0_6_f string, x_q6m_bh2_6_12_f string, x_q6m_bh2_12_18_f string, x_q6m_bh2_18_24_f string, x_q6m_bh2_0_6_total string, x_q6m_bh2_6_12_total string, x_q6m_bh2_12_18_total string, x_q6m_bh2_18_24_total string, Probabilidade_rl string, maior_F_MB string, maior_total_MB string, maior_F string, maior_total string, maior_total_MB_TODOS string)
row format delimited
fields terminated by "\,"
location '/user/hive/warehouse/dcd.db/cmr_fran/faixas_hora/df_noite';

#Passar as variaveis para numeros e excluir a 1ª linha, e 1ª coluna (id do cliente)
create table df_madru as
select numero_cpf_cnpj,
cast(numero_agencia as int) as numero_agencia,
cast(id_codigo_segmento as int) as id_codigo_segmento,
cast(id_codigo_segmento2 as int) as id_codigo_segmento2,
cast(x_q4m_bh2_0_6_mb_f_s as int) as x_q4m_bh2_0_6_mb_f_s,
cast(x_q4m_bh2_6_12_mb_f_s as int) as x_q4m_bh2_6_12_mb_f_s,
cast(x_q4m_bh2_12_18_mb_f_s as int) as x_q4m_bh2_12_18_mb_f_s,
cast(x_q4m_bh2_18_24_mb_f_s as int) as x_q4m_bh2_18_24_mb_f_s,
cast(x_q4m_bh2_0_6_mb_total_s as int) as x_q4m_bh2_0_6_mb_total_s,
cast(x_q4m_bh2_6_12_mb_total_s as int) as x_q4m_bh2_6_12_mb_total_s,
cast(x_q4m_bh2_12_18_mb_total_s as int) as x_q4m_bh2_12_18_mb_total_s,
cast(x_q4m_bh2_18_24_mb_total_s as int) as x_q4m_bh2_18_24_mb_total_s,
cast(x_q4m_bh2_0_6_f_s as int) as x_q4m_bh2_0_6_f_s,
cast(x_q4m_bh2_6_12_f_s as int) as x_q4m_bh2_6_12_f_s,
cast(x_q4m_bh2_12_18_f_s as int) as x_q4m_bh2_12_18_f_s,
cast(x_q4m_bh2_18_24_f_s as int) as x_q4m_bh2_18_24_f_s,
cast(x_q4m_bh2_0_6_total_s as int) as x_q4m_bh2_0_6_total_s,
cast(x_q4m_bh2_6_12_total_s as int) as x_q4m_bh2_6_12_total_s,
cast(x_q4m_bh2_12_18_total_s as int) as x_q4m_bh2_12_18_total_s,
cast(x_q4m_bh2_18_24_total_s as int) as x_q4m_bh2_18_24_total_s,
cast(x_q6m_bh2_0_6_mb_f as int) as x_q6m_bh2_0_6_mb_f,
cast(x_q6m_bh2_6_12_mb_f as int) as x_q6m_bh2_6_12_mb_f,
cast(x_q6m_bh2_12_18_mb_f as int) as x_q6m_bh2_12_18_mb_f,
cast(x_q6m_bh2_18_24_mb_f as int) as x_q6m_bh2_18_24_mb_f,
cast(x_q6m_bh2_0_6_mb_total as int) as x_q6m_bh2_0_6_mb_total,
cast(x_q6m_bh2_6_12_mb_total as int) as x_q6m_bh2_6_12_mb_total,
cast(x_q6m_bh2_12_18_mb_total as int) as x_q6m_bh2_12_18_mb_total,
cast(x_q6m_bh2_18_24_mb_total as int) as x_q6m_bh2_18_24_mb_total,
cast(x_q6m_bh2_0_6_f as int) as x_q6m_bh2_0_6_f,
cast(x_q6m_bh2_6_12_f as int) as x_q6m_bh2_6_12_f,
cast(x_q6m_bh2_12_18_f as int) as x_q6m_bh2_12_18_f,
cast(x_q6m_bh2_18_24_f as int) as x_q6m_bh2_18_24_f,
cast(x_q6m_bh2_0_6_total as int) as x_q6m_bh2_0_6_total,
cast(x_q6m_bh2_6_12_total as int) as x_q6m_bh2_6_12_total,
cast(x_q6m_bh2_12_18_total as int) as x_q6m_bh2_12_18_total,
cast(x_q6m_bh2_18_24_total as int) as x_q6m_bh2_18_24_total,
cast(Probabilidade_rl as double) as Probabilidade_rl,
cast(maior_F_MB as int) as maior_F_MB,
cast(maior_total_MB as int) as maior_total_MB,
cast(maior_F as int) as maior_F,
cast(maior_total as int) as maior_total,
cast(maior_total_MB_TODOS as int) as maior_total_MB_TODOS
from df_madru_1
where numero_cpf_cnpj != 'numero_cpf_cnpj';

create table df_manha as
select numero_cpf_cnpj,
cast(numero_agencia as int) as numero_agencia,
cast(id_codigo_segmento as int) as id_codigo_segmento,
cast(id_codigo_segmento2 as int) as id_codigo_segmento2,
cast(x_q4m_bh2_0_6_mb_f_s as int) as x_q4m_bh2_0_6_mb_f_s,
cast(x_q4m_bh2_6_12_mb_f_s as int) as x_q4m_bh2_6_12_mb_f_s,
cast(x_q4m_bh2_12_18_mb_f_s as int) as x_q4m_bh2_12_18_mb_f_s,
cast(x_q4m_bh2_18_24_mb_f_s as int) as x_q4m_bh2_18_24_mb_f_s,
cast(x_q4m_bh2_0_6_mb_total_s as int) as x_q4m_bh2_0_6_mb_total_s,
cast(x_q4m_bh2_6_12_mb_total_s as int) as x_q4m_bh2_6_12_mb_total_s,
cast(x_q4m_bh2_12_18_mb_total_s as int) as x_q4m_bh2_12_18_mb_total_s,
cast(x_q4m_bh2_18_24_mb_total_s as int) as x_q4m_bh2_18_24_mb_total_s,
cast(x_q4m_bh2_0_6_f_s as int) as x_q4m_bh2_0_6_f_s,
cast(x_q4m_bh2_6_12_f_s as int) as x_q4m_bh2_6_12_f_s,
cast(x_q4m_bh2_12_18_f_s as int) as x_q4m_bh2_12_18_f_s,
cast(x_q4m_bh2_18_24_f_s as int) as x_q4m_bh2_18_24_f_s,
cast(x_q4m_bh2_0_6_total_s as int) as x_q4m_bh2_0_6_total_s,
cast(x_q4m_bh2_6_12_total_s as int) as x_q4m_bh2_6_12_total_s,
cast(x_q4m_bh2_12_18_total_s as int) as x_q4m_bh2_12_18_total_s,
cast(x_q4m_bh2_18_24_total_s as int) as x_q4m_bh2_18_24_total_s,
cast(x_q6m_bh2_0_6_mb_f as int) as x_q6m_bh2_0_6_mb_f,
cast(x_q6m_bh2_6_12_mb_f as int) as x_q6m_bh2_6_12_mb_f,
cast(x_q6m_bh2_12_18_mb_f as int) as x_q6m_bh2_12_18_mb_f,
cast(x_q6m_bh2_18_24_mb_f as int) as x_q6m_bh2_18_24_mb_f,
cast(x_q6m_bh2_0_6_mb_total as int) as x_q6m_bh2_0_6_mb_total,
cast(x_q6m_bh2_6_12_mb_total as int) as x_q6m_bh2_6_12_mb_total,
cast(x_q6m_bh2_12_18_mb_total as int) as x_q6m_bh2_12_18_mb_total,
cast(x_q6m_bh2_18_24_mb_total as int) as x_q6m_bh2_18_24_mb_total,
cast(x_q6m_bh2_0_6_f as int) as x_q6m_bh2_0_6_f,
cast(x_q6m_bh2_6_12_f as int) as x_q6m_bh2_6_12_f,
cast(x_q6m_bh2_12_18_f as int) as x_q6m_bh2_12_18_f,
cast(x_q6m_bh2_18_24_f as int) as x_q6m_bh2_18_24_f,
cast(x_q6m_bh2_0_6_total as int) as x_q6m_bh2_0_6_total,
cast(x_q6m_bh2_6_12_total as int) as x_q6m_bh2_6_12_total,
cast(x_q6m_bh2_12_18_total as int) as x_q6m_bh2_12_18_total,
cast(x_q6m_bh2_18_24_total as int) as x_q6m_bh2_18_24_total,
cast(Probabilidade_rl as double) as Probabilidade_rl,
cast(maior_F_MB as int) as maior_F_MB,
cast(maior_total_MB as int) as maior_total_MB,
cast(maior_F as int) as maior_F,
cast(maior_total as int) as maior_total,
cast(maior_total_MB_TODOS as int) as maior_total_MB_TODOS
from df_manha_1
where numero_cpf_cnpj != 'numero_cpf_cnpj';

create table df_tarde as
select numero_cpf_cnpj,
cast(numero_agencia as int) as numero_agencia,
cast(id_codigo_segmento as int) as id_codigo_segmento,
cast(id_codigo_segmento2 as int) as id_codigo_segmento2,
cast(x_q4m_bh2_0_6_mb_f_s as int) as x_q4m_bh2_0_6_mb_f_s,
cast(x_q4m_bh2_6_12_mb_f_s as int) as x_q4m_bh2_6_12_mb_f_s,
cast(x_q4m_bh2_12_18_mb_f_s as int) as x_q4m_bh2_12_18_mb_f_s,
cast(x_q4m_bh2_18_24_mb_f_s as int) as x_q4m_bh2_18_24_mb_f_s,
cast(x_q4m_bh2_0_6_mb_total_s as int) as x_q4m_bh2_0_6_mb_total_s,
cast(x_q4m_bh2_6_12_mb_total_s as int) as x_q4m_bh2_6_12_mb_total_s,
cast(x_q4m_bh2_12_18_mb_total_s as int) as x_q4m_bh2_12_18_mb_total_s,
cast(x_q4m_bh2_18_24_mb_total_s as int) as x_q4m_bh2_18_24_mb_total_s,
cast(x_q4m_bh2_0_6_f_s as int) as x_q4m_bh2_0_6_f_s,
cast(x_q4m_bh2_6_12_f_s as int) as x_q4m_bh2_6_12_f_s,
cast(x_q4m_bh2_12_18_f_s as int) as x_q4m_bh2_12_18_f_s,
cast(x_q4m_bh2_18_24_f_s as int) as x_q4m_bh2_18_24_f_s,
cast(x_q4m_bh2_0_6_total_s as int) as x_q4m_bh2_0_6_total_s,
cast(x_q4m_bh2_6_12_total_s as int) as x_q4m_bh2_6_12_total_s,
cast(x_q4m_bh2_12_18_total_s as int) as x_q4m_bh2_12_18_total_s,
cast(x_q4m_bh2_18_24_total_s as int) as x_q4m_bh2_18_24_total_s,
cast(x_q6m_bh2_0_6_mb_f as int) as x_q6m_bh2_0_6_mb_f,
cast(x_q6m_bh2_6_12_mb_f as int) as x_q6m_bh2_6_12_mb_f,
cast(x_q6m_bh2_12_18_mb_f as int) as x_q6m_bh2_12_18_mb_f,
cast(x_q6m_bh2_18_24_mb_f as int) as x_q6m_bh2_18_24_mb_f,
cast(x_q6m_bh2_0_6_mb_total as int) as x_q6m_bh2_0_6_mb_total,
cast(x_q6m_bh2_6_12_mb_total as int) as x_q6m_bh2_6_12_mb_total,
cast(x_q6m_bh2_12_18_mb_total as int) as x_q6m_bh2_12_18_mb_total,
cast(x_q6m_bh2_18_24_mb_total as int) as x_q6m_bh2_18_24_mb_total,
cast(x_q6m_bh2_0_6_f as int) as x_q6m_bh2_0_6_f,
cast(x_q6m_bh2_6_12_f as int) as x_q6m_bh2_6_12_f,
cast(x_q6m_bh2_12_18_f as int) as x_q6m_bh2_12_18_f,
cast(x_q6m_bh2_18_24_f as int) as x_q6m_bh2_18_24_f,
cast(x_q6m_bh2_0_6_total as int) as x_q6m_bh2_0_6_total,
cast(x_q6m_bh2_6_12_total as int) as x_q6m_bh2_6_12_total,
cast(x_q6m_bh2_12_18_total as int) as x_q6m_bh2_12_18_total,
cast(x_q6m_bh2_18_24_total as int) as x_q6m_bh2_18_24_total,
cast(Probabilidade_rl as double) as Probabilidade_rl,
cast(maior_F_MB as int) as maior_F_MB,
cast(maior_total_MB as int) as maior_total_MB,
cast(maior_F as int) as maior_F,
cast(maior_total as int) as maior_total,
cast(maior_total_MB_TODOS as int) as maior_total_MB_TODOS
from df_tarde_1
where numero_cpf_cnpj != 'numero_cpf_cnpj';

create table df_noite as
select numero_cpf_cnpj,
cast(numero_agencia as int) as numero_agencia,
cast(id_codigo_segmento as int) as id_codigo_segmento,
cast(id_codigo_segmento2 as int) as id_codigo_segmento2,
cast(x_q4m_bh2_0_6_mb_f_s as int) as x_q4m_bh2_0_6_mb_f_s,
cast(x_q4m_bh2_6_12_mb_f_s as int) as x_q4m_bh2_6_12_mb_f_s,
cast(x_q4m_bh2_12_18_mb_f_s as int) as x_q4m_bh2_12_18_mb_f_s,
cast(x_q4m_bh2_18_24_mb_f_s as int) as x_q4m_bh2_18_24_mb_f_s,
cast(x_q4m_bh2_0_6_mb_total_s as int) as x_q4m_bh2_0_6_mb_total_s,
cast(x_q4m_bh2_6_12_mb_total_s as int) as x_q4m_bh2_6_12_mb_total_s,
cast(x_q4m_bh2_12_18_mb_total_s as int) as x_q4m_bh2_12_18_mb_total_s,
cast(x_q4m_bh2_18_24_mb_total_s as int) as x_q4m_bh2_18_24_mb_total_s,
cast(x_q4m_bh2_0_6_f_s as int) as x_q4m_bh2_0_6_f_s,
cast(x_q4m_bh2_6_12_f_s as int) as x_q4m_bh2_6_12_f_s,
cast(x_q4m_bh2_12_18_f_s as int) as x_q4m_bh2_12_18_f_s,
cast(x_q4m_bh2_18_24_f_s as int) as x_q4m_bh2_18_24_f_s,
cast(x_q4m_bh2_0_6_total_s as int) as x_q4m_bh2_0_6_total_s,
cast(x_q4m_bh2_6_12_total_s as int) as x_q4m_bh2_6_12_total_s,
cast(x_q4m_bh2_12_18_total_s as int) as x_q4m_bh2_12_18_total_s,
cast(x_q4m_bh2_18_24_total_s as int) as x_q4m_bh2_18_24_total_s,
cast(x_q6m_bh2_0_6_mb_f as int) as x_q6m_bh2_0_6_mb_f,
cast(x_q6m_bh2_6_12_mb_f as int) as x_q6m_bh2_6_12_mb_f,
cast(x_q6m_bh2_12_18_mb_f as int) as x_q6m_bh2_12_18_mb_f,
cast(x_q6m_bh2_18_24_mb_f as int) as x_q6m_bh2_18_24_mb_f,
cast(x_q6m_bh2_0_6_mb_total as int) as x_q6m_bh2_0_6_mb_total,
cast(x_q6m_bh2_6_12_mb_total as int) as x_q6m_bh2_6_12_mb_total,
cast(x_q6m_bh2_12_18_mb_total as int) as x_q6m_bh2_12_18_mb_total,
cast(x_q6m_bh2_18_24_mb_total as int) as x_q6m_bh2_18_24_mb_total,
cast(x_q6m_bh2_0_6_f as int) as x_q6m_bh2_0_6_f,
cast(x_q6m_bh2_6_12_f as int) as x_q6m_bh2_6_12_f,
cast(x_q6m_bh2_12_18_f as int) as x_q6m_bh2_12_18_f,
cast(x_q6m_bh2_18_24_f as int) as x_q6m_bh2_18_24_f,
cast(x_q6m_bh2_0_6_total as int) as x_q6m_bh2_0_6_total,
cast(x_q6m_bh2_6_12_total as int) as x_q6m_bh2_6_12_total,
cast(x_q6m_bh2_12_18_total as int) as x_q6m_bh2_12_18_total,
cast(x_q6m_bh2_18_24_total as int) as x_q6m_bh2_18_24_total,
cast(Probabilidade_rl as double) as Probabilidade_rl,
cast(maior_F_MB as int) as maior_F_MB,
cast(maior_total_MB as int) as maior_total_MB,
cast(maior_F as int) as maior_F,
cast(maior_total as int) as maior_total,
cast(maior_total_MB_TODOS as int) as maior_total_MB_TODOS
from df_noite_1
where numero_cpf_cnpj != 'numero_cpf_cnpj';


drop table df_madru_1 purge;
drop table df_manha_1 purge;
drop table df_noite_1 purge;
drop table df_tarde_1 purge;

----------------------------------------------------------------------------------------------------------------------------------

#Criando dummie, tabela com 48389 cpfs de propensos
create table df_faixa_hora_madru as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS,
(case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as madru_total
from df_madru;

#Criando dummie, tabela com 482111 cpfs de propensos
create table df_faixa_hora_manha as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS,
(case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as manha_total
from df_manha;

#Criando dummie, tabela com 665629 cpfs de propensos
create table df_faixa_hora_tarde as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS,
(case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as tarde_total
from df_tarde;

#Criando dummie, tabela com 457094 cpfs de propensos
create table df_faixa_hora_noite as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS,
(case when numero_cpf_cnpj != "123456789" then 1 else 0 end) as noite_total
from df_noite;

drop table df_madru purge;
drop table df_manha purge;
drop table df_tarde purge;
drop table df_noite purge;


#Fazendo join com todos os cpfs e colocar 0 para os clientes não propensos
create table df_faixas_hora_1 as
select A.numero_cpf_cnpj, B.madru_total, C.manha_total, D.tarde_total, E.noite_total
from 7kk_fixa_log_cred_hab_dummies_y_2 as A
left join df_faixa_hora_madru as B on A.numero_cpf_cnpj = B.numero_cpf_cnpj
left join df_faixa_hora_manha as C on A.numero_cpf_cnpj = C.numero_cpf_cnpj
left join df_faixa_hora_tarde as D on A.numero_cpf_cnpj = D.numero_cpf_cnpj
left join df_faixa_hora_noite as E on A.numero_cpf_cnpj = E.numero_cpf_cnpj;

create table df_faixas_hora_2 as
select numero_cpf_cnpj, 
(case when madru_total is not null then 1 else 0 end) as madru_total,
(case when manha_total is not null then 1 else 0 end) as manha_total,
(case when tarde_total is not null then 1 else 0 end) as tarde_total,
(case when noite_total is not null then 1 else 0 end) as noite_total
from df_faixas_hora_1;

#Verificando em quantas faixas horarias o cliente é propenso
create table df_faixas_hora_3 as
select numero_cpf_cnpj, madru_total, manha_total, tarde_total, noite_total,
(madru_total + manha_total + tarde_total + noite_total) as qnt_faixa_prop
from df_faixas_hora_2;

#Join com as variaveis
create table df_faixas_hora_4 as
select A.numero_cpf_cnpj, B.numero_agencia, B.id_codigo_segmento, B.id_codigo_segmento2, B.x_q4m_bh2_0_6_mb_f_s, B.x_q4m_bh2_6_12_mb_f_s, B.x_q4m_bh2_12_18_mb_f_s, B.x_q4m_bh2_18_24_mb_f_s, B.x_q4m_bh2_0_6_mb_total_s, B.x_q4m_bh2_6_12_mb_total_s, B.x_q4m_bh2_12_18_mb_total_s, B.x_q4m_bh2_18_24_mb_total_s, B.x_q4m_bh2_0_6_f_s, B.x_q4m_bh2_6_12_f_s, B.x_q4m_bh2_12_18_f_s, B.x_q4m_bh2_18_24_f_s, B.x_q4m_bh2_0_6_total_s, B.x_q4m_bh2_6_12_total_s, B.x_q4m_bh2_12_18_total_s, B.x_q4m_bh2_18_24_total_s, B.x_q6m_bh2_0_6_mb_f, B.x_q6m_bh2_6_12_mb_f, B.x_q6m_bh2_12_18_mb_f, B.x_q6m_bh2_18_24_mb_f, B.x_q6m_bh2_0_6_mb_total, B.x_q6m_bh2_6_12_mb_total, B.x_q6m_bh2_12_18_mb_total, B.x_q6m_bh2_18_24_mb_total, B.x_q6m_bh2_0_6_f, B.x_q6m_bh2_6_12_f, B.x_q6m_bh2_12_18_f, B.x_q6m_bh2_18_24_f, B.x_q6m_bh2_0_6_total, B.x_q6m_bh2_6_12_total, B.x_q6m_bh2_12_18_total, B.x_q6m_bh2_18_24_total, B.probabilidade_rl, B.maior_f_mb, B.maior_total_mb, B.maior_f, B.maior_total, B.maior_total_mb_todos, A.madru_total, A.manha_total, A.tarde_total, A.noite_total, A.qnt_faixa_prop
from df_faixas_hora_3 as A left join
df_faixa_hora_tarde as B on A.numero_cpf_cnpj = B.numero_cpf_cnpj;


#Intersecao dos 4 horarios
create table df_faixas_hora_5 as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS, madru_total, manha_total, tarde_total, noite_total, qnt_faixa_prop,
case when qnt_faixa_prop = 4 then 1 else 0 end as madru_manha_tarde_noite
from df_faixas_hora_4;

#Intersecao de 3 horarios
create table df_faixas_hora_6 as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS, madru_total, manha_total, tarde_total, noite_total, qnt_faixa_prop, madru_manha_tarde_noite,
case when qnt_faixa_prop = 3 and madru_total = 1 and manha_total = 1 and tarde_total = 1 then 1 else 0 end as madru_manha_tarde,
case when qnt_faixa_prop = 3 and madru_total = 1 and manha_total = 1 and noite_total = 1 then 1 else 0 end as madru_manha_noite,
case when qnt_faixa_prop = 3 and madru_total = 1 and tarde_total = 1 and noite_total = 1 then 1 else 0 end as madru_tarde_noite,
case when qnt_faixa_prop = 3 and manha_total = 1 and tarde_total = 1 and noite_total = 1 then 1 else 0 end as manha_tarde_noite
from df_faixas_hora_5;

#Intersecao de 2 horarios
create table df_faixas_hora_7 as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS, madru_total, manha_total, tarde_total, noite_total, qnt_faixa_prop, madru_manha_tarde_noite, madru_manha_tarde, madru_manha_noite, madru_tarde_noite, manha_tarde_noite,
case when qnt_faixa_prop = 2 and madru_total = 1 and manha_total = 1 then 1 else 0 end as madru_manha,
case when qnt_faixa_prop = 2 and madru_total = 1 and tarde_total = 1 then 1 else 0 end as madru_tarde,
case when qnt_faixa_prop = 2 and madru_total = 1 and noite_total = 1 then 1 else 0 end as madru_noite,
case when qnt_faixa_prop = 2 and manha_total = 1 and tarde_total = 1 then 1 else 0 end as manha_tarde,
case when qnt_faixa_prop = 2 and manha_total = 1 and noite_total = 1 then 1 else 0 end as manha_noite,
case when qnt_faixa_prop = 2 and tarde_total = 1 and noite_total = 1 then 1 else 0 end as tarde_noite
from df_faixas_hora_6;

#Apenas em uma faixa de hora (Tabela Final)
create table df_faixas_hora as
select numero_cpf_cnpj, numero_agencia, id_codigo_segmento, id_codigo_segmento2, x_q4m_bh2_0_6_mb_f_s, x_q4m_bh2_6_12_mb_f_s, x_q4m_bh2_12_18_mb_f_s, x_q4m_bh2_18_24_mb_f_s, x_q4m_bh2_0_6_mb_total_s, x_q4m_bh2_6_12_mb_total_s, x_q4m_bh2_12_18_mb_total_s, x_q4m_bh2_18_24_mb_total_s, x_q4m_bh2_0_6_f_s, x_q4m_bh2_6_12_f_s, x_q4m_bh2_12_18_f_s, x_q4m_bh2_18_24_f_s, x_q4m_bh2_0_6_total_s, x_q4m_bh2_6_12_total_s, x_q4m_bh2_12_18_total_s, x_q4m_bh2_18_24_total_s, x_q6m_bh2_0_6_mb_f, x_q6m_bh2_6_12_mb_f, x_q6m_bh2_12_18_mb_f, x_q6m_bh2_18_24_mb_f, x_q6m_bh2_0_6_mb_total, x_q6m_bh2_6_12_mb_total, x_q6m_bh2_12_18_mb_total, x_q6m_bh2_18_24_mb_total, x_q6m_bh2_0_6_f, x_q6m_bh2_6_12_f, x_q6m_bh2_12_18_f, x_q6m_bh2_18_24_f, x_q6m_bh2_0_6_total, x_q6m_bh2_6_12_total, x_q6m_bh2_12_18_total, x_q6m_bh2_18_24_total, Probabilidade_rl, maior_F_MB, maior_total_MB, maior_F, maior_total, maior_total_MB_TODOS, madru_total, manha_total, tarde_total, noite_total, qnt_faixa_prop, madru_manha_tarde_noite, madru_manha_tarde, madru_manha_noite, madru_tarde_noite, manha_tarde_noite, madru_manha, madru_tarde, madru_noite, manha_tarde, manha_noite, tarde_noite,
case when qnt_faixa_prop = 1 and madru_total = 1 then 1 else 0 end as madru_apenas,
case when qnt_faixa_prop = 1 and manha_total = 1 then 1 else 0 end as manha_apenas,
case when qnt_faixa_prop = 1 and tarde_total = 1 then 1 else 0 end as tarde_apenas,
case when qnt_faixa_prop = 1 and noite_total = 1 then 1 else 0 end as noite_apenas
from df_faixas_hora_7;


drop table df_faixas_hora_1 purge;
drop table df_faixas_hora_2 purge;
drop table df_faixas_hora_3 purge;
drop table df_faixas_hora_4 purge;
drop table df_faixas_hora_5 purge;
drop table df_faixas_hora_6 purge;
drop table df_faixas_hora_7 purge;

-------------Conferindo os numeros----------------------------------------------------------------------------------------------

select count (madru_manha_tarde_noite) from df_faixas_hora where madru_manha_tarde_noite = 1;
select count (madru_manha_tarde) from df_faixas_hora where madru_manha_tarde = 1;
select count (madru_manha_noite) from df_faixas_hora where madru_manha_noite = 1;
select count (madru_tarde_noite) from df_faixas_hora where madru_tarde_noite = 1;
select count (manha_tarde_noite) from df_faixas_hora where manha_tarde_noite = 1;
select count (madru_manha) from df_faixas_hora where madru_manha = 1;
select count (madru_tarde) from df_faixas_hora where madru_tarde = 1;
select count (madru_noite) from df_faixas_hora where madru_noite = 1;
select count (manha_tarde) from df_faixas_hora where manha_tarde = 1;
select count (manha_noite) from df_faixas_hora where manha_noite = 1;
select count (tarde_noite) from df_faixas_hora where tarde_noite = 1;
select count (madru_apenas) from df_faixas_hora where madru_apenas = 1;
select count (manha_apenas) from df_faixas_hora where manha_apenas = 1;
select count (tarde_apenas) from df_faixas_hora where tarde_apenas = 1;
select count (noite_apenas) from df_faixas_hora where noite_apenas = 1;

#Quantidade para prencher no slide
madru_manha_tarde_noite : 6736
madru_manha_tarde : 2487
madru_manha_noite : 2120
madru_tarde_noite : 2752
manha_tarde_noite : 16959
madru_manha : 6280
madru_tarde : 9748
madru_noite : 7738
manha_tarde : 66540
manha_noite : 43492
tarde_noite : 62303
madru_apenas : 10528
manha_apenas : 337497
tarde_apenas : 498104
noite_apenas : 314994
