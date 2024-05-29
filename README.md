UDF
任务1：自定义UDF函数判断员工表中的工资级别。

· sal <= 1000，工资级别为Grade_A；

· 1000 < sal <= 3000，工资级别为Grade_B；

· 3000 < sal，工资级别为Grade_C。


UTAF
任务2：自定义UDAF函数返回该列中所有字符串的字符总数。


UDTF
任务3：自定义UDTF函数用来解析key:value:value形式的字符串。

原数据：1:a:A;2:b:B;3:c:C;4:d:D;

结果数据：

id lower upper

1   a   A

2   b   B

3   c   C
