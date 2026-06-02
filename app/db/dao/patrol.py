from app.db.dao.base import BaseDAO
from app.db.models.patrol import PatrolTaskDetORM, PatrolTaskORM


class PatrolTaskDAO(BaseDAO):
    model = PatrolTaskORM


class PatrolTaskDetDAO(BaseDAO):
    model = PatrolTaskDetORM
