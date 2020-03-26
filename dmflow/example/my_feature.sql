-- 一个使用 SQL_Assembler 读取特征的例子
select
    pid
    , first(age) as age
from db.user_info
where concat(year,month,day) = '${HIVE_COMPACT_DATE}'
    and pid is not null
    and age > ${AGE}
group by pid