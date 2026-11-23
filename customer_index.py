"""
Индекс всех AbstractCustomer в памяти
Строится один раз перед синхронизацией, чтобы не делать запрос на каждую сущность
"""
import logging
from collections import defaultdict
from typing import Optional, Dict, List

from sqlalchemy import select

from models import AbstractCustomer, db

logger = logging.getLogger(__name__)


class CustomerIndex:
    def __init__(self):
        self.by_lead_id: Dict[int, AbstractCustomer] = {}
        self.by_customer_id: Dict[int, AbstractCustomer] = {}
        self.by_lead_login: Dict[str, List[AbstractCustomer]] = defaultdict(list)
        self.by_customer_login: Dict[str, List[AbstractCustomer]] = defaultdict(list)
        self.all: List[AbstractCustomer] = []


    @classmethod
    def load(cls) -> "CustomerIndex":
        idx = cls()
        customers = db.session.scalars(select(AbstractCustomer)).all()
        for ac in customers:
            idx.add(ac)
        logger.info(f"CustomerIndex loaded: {len(idx.all)} abstract customers, "
                    f"leads={len(idx.by_lead_id)}, customers={len(idx.by_customer_id)}")
        return idx


    def add(self, ac: AbstractCustomer):
        if ac not in self.all:
            self.all.append(ac)
        for lead in (ac.amo_leads or []):
            g = lead.get("general") or {}
            if g.get("id") is not None:
                self.by_lead_id[g["id"]] = ac
            if g.get("login"):
                self.by_lead_login[g["login"]].append(ac)
        for cust in (ac.amo_customers or []):
            g = cust.get("general") or {}
            if g.get("id") is not None:
                self.by_customer_id[g["id"]] = ac
            if g.get("login"):
                self.by_customer_login[g["login"]].append(ac)


    def remove(self, ac: AbstractCustomer):
        if ac in self.all:
            self.all.remove(ac)
        for lead in (ac.amo_leads or []):
            g = lead.get("general") or {}
            if g.get("id") is not None and self.by_lead_id.get(g["id"]) is ac:
                self.by_lead_id.pop(g["id"], None)
            if g.get("login") and ac in self.by_lead_login.get(g["login"], []):
                self.by_lead_login[g["login"]].remove(ac)
        for cust in (ac.amo_customers or []):
            g = cust.get("general") or {}
            if g.get("id") is not None and self.by_customer_id.get(g["id"]) is ac:
                self.by_customer_id.pop(g["id"], None)
            if g.get("login") and ac in self.by_customer_login.get(g["login"], []):
                self.by_customer_login[g["login"]].remove(ac)


    def reindex(self, ac: AbstractCustomer):
        self.by_lead_id = {k: v for k, v in self.by_lead_id.items() if v is not ac}
        self.by_customer_id = {k: v for k, v in self.by_customer_id.items() if v is not ac}
        for k in list(self.by_lead_login):
            self.by_lead_login[k] = [x for x in self.by_lead_login[k] if x is not ac]
        for k in list(self.by_customer_login):
            self.by_customer_login[k] = [x for x in self.by_customer_login[k] if x is not ac]
        if ac in self.all:
            self.all.remove(ac)
        self.add(ac)


    def get_by_lead_id(self, lead_id: int) -> Optional[AbstractCustomer]:
        return self.by_lead_id.get(lead_id)


    def get_by_customer_id(self, customer_id: int) -> Optional[AbstractCustomer]:
        return self.by_customer_id.get(customer_id)


    def _pick_unique(self, items: List[AbstractCustomer], what: str) -> Optional[AbstractCustomer]:
        if len(items) > 1:
            logger.warning(f"{what} has more than one abccustomer")
            return None
        return items[0] if items else None


    def get_by_lead_login(self, login: str) -> Optional[AbstractCustomer]:
        return self._pick_unique(self.by_lead_login.get(login, []), f"Lead {login}")


    def get_by_customer_login(self, login: str) -> Optional[AbstractCustomer]:
        return self._pick_unique(self.by_customer_login.get(login, []), f"Customer {login}")