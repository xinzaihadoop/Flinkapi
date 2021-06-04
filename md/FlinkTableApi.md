Flink数据更新模式：
    1、追加append模式：
    表只做插入操作，和外部的连接器只交换插入消息
    2、撤回Retract模式：
    - 表和外部连结器交换添加add和撤回Retract消息
    - 插入insert操作编码为add消息，删除delete编码为retract
    - 更新的编码为上一条的retract和下一条的add消息
    3、更新传入Upsert模式：
    - 更新和插入编码都为Upsert消息，删除编码为Delete消息

