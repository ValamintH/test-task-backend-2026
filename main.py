import logging
from typing import Iterator, List, Dict

from sqlalchemy import delete
from sqlalchemy.orm.attributes import flag_modified

from abstract_customer import create_or_update_customer, dict_compare_merge, create_or_update_lead
from amocrm import Customer, COMPANIES, CONTACTS, Lead, amocrm_get
from customer_index import CustomerIndex
from models import db, AbstractCustomer

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000


def _maybe_commit(counter: int, batch_size: int = BATCH_SIZE) -> int:
    """Коммитит, если счётчик достиг размера батча. Возвращает новый счётчик"""
    if counter >= batch_size:
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise
        return 0
    return counter


def lead_action(lead: Lead, index: CustomerIndex):
    abstract_customer = index.get_by_lead_id(lead.id)

    lead_login = lead.get_field('Логин')
    if lead_login:
        abs_customer_login = index.get_by_customer_login(lead_login)
        if abstract_customer is None:
            abstract_customer = abs_customer_login
        if abs_customer_login is not None:
            if abs_customer_login.id != abstract_customer.id:
                abstract_customer.amo_customers = dict_compare_merge(abstract_customer.amo_customers or [],
                                                                     abs_customer_login.amo_customers or [])
                abstract_customer.amo_leads = dict_compare_merge(abstract_customer.amo_leads or [],
                                                                 abs_customer_login.amo_leads or [])
                abstract_customer.amo_contacts = dict_compare_merge(abstract_customer.amo_contacts or [],
                                                                    abs_customer_login.amo_contacts or [])
                abstract_customer.amo_companies = dict_compare_merge(abstract_customer.amo_companies or [],
                                                                     abs_customer_login.amo_companies or [])
                flag_modified(abstract_customer, "amo_customers")
                flag_modified(abstract_customer, "amo_leads")
                flag_modified(abstract_customer, "amo_contacts")
                flag_modified(abstract_customer, "amo_companies")
                db.session.flush()
                db.session.delete(abs_customer_login)
                db.session.flush()
                index.remove(abs_customer_login)
                index.reindex(abstract_customer)
                return abstract_customer

    result = create_or_update_lead(abstract_customer, lead)
    index.reindex(result)
    return result


def collect_leads_customers(is_full=True):
    """
     Берем всех покупателей, если его нет в бд - делаем новую запись AbstractCutomer, в него кладем.
     Есть есть - находим его запись по id и перезаписываем

     Вот так у нас коллектятся все кастомеры

     Далее берутся все лиды. Ищутся их записи в бд по id, если не находится и есть логин
     - находим его запись по кастомеру с тем же логином, кладем к нему.
     Если нет логина/нет кастомера - пропускаем. Если нашелся - перезаписываем соответственно
         abstract_id_customer : {
        customers: [...],
        leads: [...],
        contacts: [...],
        companies: [...]
    }
    """
    # Один запрос для всего
    index = CustomerIndex.load()

    customers: Iterator[Customer] = amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}")
    logger.info(f"Collected customers {is_full=}")

    ids_map = {}
    batch_counter = 0
    for customer in customers:
        if is_full is False and customer.status_id == 111349:
            continue
        customer_login = customer.get_field('Логин')
        if not customer_login:
            continue
        abstract_customer = index.get_by_customer_id(customer.id)

        if not abstract_customer:
            abstract_customer = index.get_by_lead_login(customer_login)  # почти всегда логины совпадают
        abstract_customer = create_or_update_customer(abstract_customer, customer)
        index.reindex(abstract_customer)
        if is_full is True:
            if abstract_customer.id in ids_map:
                ids_map[abstract_customer.id]["customers"].append(customer.id)
                ids_map[abstract_customer.id]["contacts"].extend([c["id"] for c in customer.contacts])
                ids_map[abstract_customer.id]["companies"].extend([c["id"] for c in customer.companies])
            else:
                ids_map[abstract_customer.id] = {
                    "customers": [customer.id],
                    "contacts": [c["id"] for c in customer.contacts],
                    "companies": [c["id"] for c in customer.companies],
                    "leads": [],
                }
        batch_counter += 1
        batch_counter = _maybe_commit(batch_counter)

    # финальный коммит батча customers
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    del customers
    amo_leads: Iterator[Lead] = amocrm_get(Lead, _with=f"{CONTACTS}")
    logger.info(f"Collected leads {is_full=}")

    batch_counter = 0
    for amo_lead in amo_leads:
        if is_full is False and amo_lead.status_id == 143:
            continue
        try:
            abc_customer = lead_action(amo_lead, index)
            if is_full is True:
                if abc_customer.id in ids_map:
                    ids_map[abc_customer.id]["leads"].append(amo_lead.id)
                    ids_map[abc_customer.id]["contacts"].extend([c["id"] for c in amo_lead.contacts])
                else:
                    ids_map[abc_customer.id] = {
                        "customers": [],
                        "contacts": [c["id"] for c in amo_lead.contacts],
                        "companies": [],
                        "leads": [amo_lead.id],
                    }
        except Exception as e:
            db.session.rollback()
            logger.exception(f'{amo_lead} {e}')
            batch_counter = 0
            continue
        batch_counter += 1
        batch_counter = _maybe_commit(batch_counter)

    # финальный коммит батча leads
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    if is_full is False:
        return
    try:
        logger.info("Чистка покупателей общая")
        stmt = delete(AbstractCustomer).where(
            AbstractCustomer.id.not_in(ids_map.keys())
        )
        db.session.execute(stmt)
        db.session.commit()
        logger.info("Чистка покупателей на уровне сущностей")

        batch_counter = 0
        for abc_customer in list(index.all):
            if abc_customer.id not in ids_map:
                continue
            try:
                filter_abc_customer(abc_customer=abc_customer, ids_map=ids_map)
            except Exception as e:
                db.session.rollback()
                logger.exception(e)
                batch_counter = 0
                continue
            batch_counter += 1
            batch_counter = _maybe_commit(batch_counter)
        # финальный коммит батча чистки
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.exception(f"Чистка: {e}")


def filter_merge_abstract_customer(abc_customer: AbstractCustomer, attr: str,
                                   check_ids: List[int]):
    entity = abc_customer.__getattribute__(attr)
    new_entity = [c for c in entity if
                  'удалить' not in c['general'].get('name').lower() and c['general'].get('id') in check_ids]
    if new_entity != entity:
        setattr(abc_customer, attr, new_entity)
        flag_modified(abc_customer, attr)
        db.session.flush()


def filter_abc_customer(abc_customer: AbstractCustomer, ids_map: Dict):
    if abc_customer.amo_leads:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_leads",
                                           check_ids=ids_map[abc_customer.id]["leads"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_customers:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_customers",
                                           check_ids=ids_map[abc_customer.id]["customers"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_contacts:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_contacts",
                                           check_ids=ids_map[abc_customer.id]["contacts"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_companies:
        try:
            filter_merge_abstract_customer(abc_customer=abc_customer,
                                           attr="amo_companies",
                                           check_ids=ids_map[abc_customer.id]["companies"])
        except Exception as e:
            logger.exception(e)
    if not abc_customer.amo_leads and not abc_customer.amo_customers:
        db.session.flush()
        db.session.delete(abc_customer)
        db.session.flush()
