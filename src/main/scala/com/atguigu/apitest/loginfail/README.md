


```$xslt
两个方案

传统方案：对同一用户（通过用户编号标识）登录状态进行统计，将状态放入ListState中，遍历ListState统计两次失败间隔时间在定义时间范围内，进行报警
CEP方案：通过CEP定义“危险pattern”，实现数据滤出
```
userId|	ip|	eventType|	eventTime
------|--|-----|------------
5402|	83.149.11.115|	success|	1558430815
