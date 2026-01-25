# Cache Proxy

> 文件反向代理工具

**此项目是 Dragon's Zone Homelab 的一部分**

此项目主要是为了缓解内网大规模更新系统导致的网络拥堵，因此未考虑各种安全加固，例如内存限制或防止缓存击穿等。

## 特性

- 反向代理任意 HTTP/HTTPS 内容
- 可修改任意回传内容 (这将导致 http range 失效)
- 重复文件合并

## TODO

- [ ] 支持 Prometheus 监控
- [ ] 支持 ACME + HTTPS
- [ ] 热点数据装入内存降低磁盘读写

## LICENSE

此项目使用 [MIT](./LICENSE)
