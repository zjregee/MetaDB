## kv数据库中的数据格式

**key**

键是key，值是0

**UUID**

键是UUID，值是0

**key:UUID**

键是key，值是UUID

**key:filename**

键是key，值是filename

**UUID:filename**

键是UUID，值是filename

**Data rules**

* key和UUID是唯一的
* 一个key只能对应一个文件
* 多个key可以对应同一个文件
* UUID和文件是一一对应的关系
* 一个filename可能有多个文件多个key
* 一个文件只能有一个filename