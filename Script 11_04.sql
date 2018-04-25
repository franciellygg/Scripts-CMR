#Passar do jupyter para o hive
#criar pasta
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_na
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_np
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_na
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_np
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_na
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_np
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_na
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_np
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c
hdfs dfs -mkdir /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t


#Copia do jupyter para o hdfs
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_00H_06H_na.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_na/641004_Lime_Enviados.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_00H_06H_np.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_np/PUSH_CRED_PARC_REC_PRIME_00H_06H_np.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_06H_12H_na.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_na/PUSH_CRED_PARC_REC_PRIME_06H_12H_na.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_06H_12H_np.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_np/PUSH_CRED_PARC_REC_PRIME_06H_12H_np.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_12H_18H_na.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_na/PUSH_CRED_PARC_REC_PRIME_12H_18H_na.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_12H_18H_np.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_np/PUSH_CRED_PARC_REC_PRIME_12H_18H_np.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_18H_24H_na.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_na/PUSH_CRED_PARC_REC_PRIME_18H_24H_na.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_18H_24H_np.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_np/PUSH_CRED_PARC_REC_PRIME_18H_24H_np.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c.csv
hdfs dfs -copyFromLocal /home/fgastardi/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t.csv /user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t.csv


create external table PUSH_CRED_PARC_REC_PRIME_00H_06H_na (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_na';

create external table PUSH_CRED_PARC_REC_PRIME_00H_06H_np (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_np';

create external table PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c';

create external table PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t';

create external table PUSH_CRED_PARC_REC_PRIME_06H_12H_na (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_na';

create external table PUSH_CRED_PARC_REC_PRIME_06H_12H_np (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_np';

create external table PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c';

create external table PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t';

create external table PUSH_CRED_PARC_REC_PRIME_12H_18H_na (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_na';

create external table PUSH_CRED_PARC_REC_PRIME_12H_18H_np (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_np';

create external table PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c';

create external table PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t';

create external table PUSH_CRED_PARC_REC_PRIME_18H_24H_na (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_na';

create external table PUSH_CRED_PARC_REC_PRIME_18H_24H_np (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_np';

create external table PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c';

create external table PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t (Agencia string, Conta string)
row format delimited
fields terminated by '\u0059'
location '/user/hive/warehouse/dcd.db/cmr_fran/Temp/PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t';



#Eliminar a primeira linha da tabela 

create table PUSH_CRED_PARC_REC_PRIME_00H_06H_na_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_00H_06H_na' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_00H_06H_na where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_00H_06H_np_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_00H_06H_np' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_00H_06H_np where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_06H_12H_na_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_06H_12H_na' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_06H_12H_na where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_06H_12H_np_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_06H_12H_np' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_06H_12H_np where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_12H_18H_na_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_12H_18H_na' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_12H_18H_na where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_12H_18H_np_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_12H_18H_np' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_12H_18H_np where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_18H_24H_na_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_18H_24H_na' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_18H_24H_na where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_18H_24H_np_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_18H_24H_np' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_18H_24H_np where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c where conta != 'conta';

create table PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t_1 as 
select agencia, conta, 'PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t' as arquivo 
from PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t where conta != 'conta';


create table PUSH_CRED_PARC_REC_PRIME_Union as
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_00H_06H_na_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_00H_06H_np_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_00H_06H_p_c_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_00H_06H_p_t_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_06H_12H_na_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_06H_12H_np_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_06H_12H_p_c_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_06H_12H_p_t_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_12H_18H_na_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_12H_18H_np_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_12H_18H_p_c_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_12H_18H_p_t_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_18H_24H_na_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_18H_24H_np_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_18H_24H_p_c_1 union all
select agencia, conta, arquivo from PUSH_CRED_PARC_REC_PRIME_18H_24H_p_t_1;


hive -e "use dcd; set hive.cli.print.header=true; select * from push_cred_parc_rec_classic_union;" > /home/fgastardi/push_cred_parc_rec_classic_union.csv
hive -e "use dcd; set hive.cli.print.header=true; select * from push_cred_parc_teste_classic_union;" > /home/fgastardi/push_cred_parc_teste_classic_union.csv
hive -e "use dcd; set hive.cli.print.header=true; select * from push_cred_parc_rec_exclusive_union;" > /home/fgastardi/push_cred_parc_rec_exclusive_union.csv
hive -e "use dcd; set hive.cli.print.header=true; select * from push_cred_parc_teste_exclusive_union;" > /home/fgastardi/push_cred_parc_teste_exclusive_union.csv
hive -e "use dcd; set hive.cli.print.header=true; select * from push_cred_parc_rec_prime_union;" > /home/fgastardi/push_cred_parc_rec_prime_union.csv
hive -e "use dcd; set hive.cli.print.header=true; select * from push_cred_parc_teste_prime_union;" > /home/fgastardi/push_cred_parc_teste_prime_union.csv


hdfs dfs -rm -r -skipTrash /user/hive/warehouse/dcd.db/cmr_fran/Temp

drop table push_cred_parc_teste_prime_00h_06h_na purge;
drop table push_cred_parc_teste_prime_00h_06h_np purge;
drop table push_cred_parc_teste_prime_00h_06h_p_c purge;
drop table push_cred_parc_teste_prime_00h_06h_p_t purge;
drop table push_cred_parc_teste_prime_06h_12h_na purge;
drop table push_cred_parc_teste_prime_06h_12h_np purge;
drop table push_cred_parc_teste_prime_06h_12h_p_c purge;
drop table push_cred_parc_teste_prime_06h_12h_p_t purge;
drop table push_cred_parc_teste_prime_12h_18h_na purge;
drop table push_cred_parc_teste_prime_12h_18h_np purge;
drop table push_cred_parc_teste_prime_12h_18h_p_c purge;
drop table push_cred_parc_teste_prime_12h_18h_p_t purge;
drop table push_cred_parc_teste_prime_18h_24h_na purge;
drop table push_cred_parc_teste_prime_18h_24h_np purge;
drop table push_cred_parc_teste_prime_18h_24h_p_c purge;
drop table push_cred_parc_teste_prime_18h_24h_p_t purge;



