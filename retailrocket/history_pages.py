"""Complete V1 analysis pages at the shared immutable HTTP snapshot boundary."""
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Literal
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from retailrocket import query


def history_router(history):
    router = APIRouter(prefix='/api/history')

    @lru_cache(maxsize=128)
    def page_data(directory, page, start, end, visitorid, page_number, page_size, top_n, scope, limit):
        if page == 'purchase':
            return query.visitor_conversion(directory, start, end) if start is not None else None
        if page == 'categories':
            if start is None:
                return None
            from retailrocket.category_query import category_analysis
            result = category_analysis(directory, start, end)
            result['items'] = result['items'][:100]
            return result
        if page == 'profiles':
            if start is None:
                return None
            from retailrocket.profile_query import visitor_profiles
            return visitor_profiles(directory, start, end, visitorid, page_number, page_size, top_n)
        if page == 'quality':
            dates = (start, end) if scope == 'range' else (None, None)
            return dict(batch_quality=query.quality_summary(directory),
                        range_quality=query.range_quality(directory, start, end) if start is not None else None,
                        reasons=query.anomaly_reasons(directory, *dates) if scope == 'batch' or start is not None else [],
                        records=query.anomaly_records(directory, *dates, limit=limit) if scope == 'batch' or start is not None else [],
                        anomaly_scope=scope)
        raise ValueError('Unknown history page')

    @router.get('/{page}')
    def history_page(page: Literal['purchase', 'categories', 'profiles', 'quality'],
                     start: date | None = None, end: date | None = None,
                     visitorid: str | None = None,
                     page_number: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100),
                     top_n: int = Query(5, ge=1, le=50),
                     scope: Literal['batch', 'range'] = 'batch', limit: int = Query(100, ge=1, le=500)):
        if (start is None) != (end is None) or (start is not None and start > end):
            raise HTTPException(422, '请选择完整且顺序正确的 UTC 日期范围')
        try:
            snapshot = history.snapshot()
            if snapshot is None:
                return dict(snapshot=None, selected_range=None, data=None)
            bounds = query.date_bounds(snapshot.directory)
            start, end = (start, end) if start is not None else bounds
            # Only verified immutable revision paths enter this bounded cache.
            data = page_data(snapshot.directory, page, start, end, visitorid, page_number, page_size, top_n, scope, limit)
            return jsonable_encoder(dict(snapshot=dict(
                batch_id=snapshot.batch_id, revision=snapshot.directory.name,
                timezone='UTC', complete=True, event_bounds=dict(start=bounds[0], end=bounds[1]),
                updated_at=datetime.fromtimestamp((snapshot.directory/'publication.json').stat().st_mtime, timezone.utc)),
                selected_range=dict(start=start,end=end), data=data))
        except (ValueError, FileNotFoundError, KeyError, OSError) as error:
            raise HTTPException(503, '已发布批次暂不可用，请检查发布存储') from error

    return router
