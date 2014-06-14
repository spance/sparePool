SparePool
=========

一个足够简单足够轻量的数据库连接池。

定位于短生命周期的连接池，一种典型情况为主连接池之外需要其它策略的任务执行等。当然也适用简单应用长生命周期等。

提供了一致简单的资源释放方法，记住free就够了！

用户可以不必关闭Statement和ResultSet，在获得Connection的线程内调用free()即可释放所有相关资源。

池不负责连接超时和泄露管理，池在关闭时将释放所有资源-Connection,Statement,ResultSet.

一系列任务完成并其它线程不再资源时可以关闭池SecondSimplePool.shutdown()
