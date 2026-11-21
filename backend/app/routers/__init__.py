"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

固定路径的管道路由必须先注册：否则 ``GET /api/{module}/{entry_id}``
会把 ``/api/pipeline/health-summary`` 这类固定路径当成资源详情。
"""
from __future__ import annotations

from app.routers import pipeline, resource

ROUTERS = [pipeline, resource]
