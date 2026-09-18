"""跨模块共用的常量（避免定义在某个路由模块里而被别处依赖）。

v3.17（2026-09-18）新建：起因是 `_APPROVAL_TTL` 原本定义在 api/app.py 的 admin 块里，
该块被抽到 api/routes_admin.py 后，留在 app.py 的插件端点引用它就变成未定义名字（NameError）。
"""

# 审批缓存 TTL（秒）：插件调用端点与 admin 的 MCP 状态端点都用它
APPROVAL_TTL = 300  # 5 分钟
