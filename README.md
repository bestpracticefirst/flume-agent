# flume-agent
flume agent, 修改了flume的结构，去掉了channel。

将flume中的source和sink收敛为了单线程，同步发送，主要用于agent的部署。
